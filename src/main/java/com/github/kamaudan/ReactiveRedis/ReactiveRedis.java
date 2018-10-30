package com.github.kamaudan.ReactiveRedis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.UUID;

@SpringBootApplication
public class ReactiveRedis {

	/* The Redis database driver here is Lettuce, since by now is the only reactive driver,
	Jedis is still blocking. An advanatge of using Lettuce is the it gives a connection factory, unlike in Jedis
	Then we instantiate the jackson object to handle our data serialization to and from json, for our coffee class
	 *  */

	@Bean
	ReactiveRedisOperations<String, Coffee> redisOperations(ReactiveRedisConnectionFactory factory){
		Jackson2JsonRedisSerializer<Coffee>  ser = new  Jackson2JsonRedisSerializer(Coffee.class);

		RedisSerializationContext.RedisSerializationContextBuilder builder = RedisSerializationContext
				.newSerializationContext(new StringRedisSerializer());

		RedisSerializationContext<String, Coffee> context =  builder.value(ser).build();

		return new ReactiveRedisTemplate<>(factory, context);

	}

	public static void main(String[] args) {
		SpringApplication.run(ReactiveRedis.class, args);
	}
}
@Service
class DataLoader {
	private final ReactiveRedisConnectionFactory factory;
	private final ReactiveRedisOperations<String, Coffee> operations;


	public DataLoader(ReactiveRedisConnectionFactory factory, ReactiveRedisOperations<String, Coffee> operations) {
		this.factory = factory;
		this.operations = operations;
	}

	@PostConstruct
	private void load(){
		/*
		We are creating a publisher 'Flux' returns a stream of results  in our case coffee,
		we instantiate our coffee object, with a unique id.
		Reactive Extension are lazy in nature, until you call a terminal function it will not perform anything.
		.subscribe is the terminal function
	*/
		factory.getReactiveConnection().serverCommands().flushAll().thenMany(
		Flux.just("Java", "Artcafe","Domans", "Americana", "Kaldi", "Green Coffee", "Coffee inn", "Kahawa number moja", "Sasini", "Nescafe")
				.map(name -> new Coffee(name, UUID.randomUUID().toString()))
				.flatMap(coffee -> operations.opsForValue().set(coffee.getId(), coffee)))
				.thenMany(operations.keys("*") .flatMap(operations.opsForValue()::get))
				.subscribe(System.out::println);




	}
}
@RestController
class CoffeeController {
	private final ReactiveRedisOperations<String, Coffee> cofops;

	public CoffeeController(ReactiveRedisOperations<String, Coffee> cofops) {
		this.cofops = cofops;
	}
	/*
	The get all method returns a stream of data 'Flux', we have to specify that MediaType produced is
	TEXT_EVENT_STREAM_VALUE, to get the data as it streams.

	 */

	@GetMapping(value = "/coffees", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<Coffee> getAllCoffees(){
		return cofops.keys("*").flatMap(cofops.opsForValue()::get)
				.delayElements(Duration.ofSeconds(1));
	}


}

@AllArgsConstructor
@NoArgsConstructor
@Data
class Coffee {
	private String name;
	private String id;

}