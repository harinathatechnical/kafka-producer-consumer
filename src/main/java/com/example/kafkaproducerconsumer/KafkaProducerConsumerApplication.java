package com.example.kafkaproducerconsumer;

import com.example.kafkaproducerconsumer.config.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@RestController
public class KafkaProducerConsumerApplication {

	@Autowired
	private KafkaTemplate<String,Object> template;

	private String topic="haritest";

	// Produce String message

	@GetMapping("/publish/{name}")
	public String publishMessage(@PathVariable String name){
		template.send(topic,"Hi "+name+ " welcome");
		return "published";
	}

	// Produce Json message

	@GetMapping("/publishJson")
	public String publishJson(){
		User user = new User(12,"test",new String[]{"Bglr","BTM"});
		template.send(topic,user);
		return "json published";
	}

	// use for consumer
	List<String> messages = new ArrayList<>();
	User userfromTopic = null;

	// use the consumer string retrieve
	@GetMapping("/consumeString")
	public List<String>  consumeMsg(){
		return messages;
	}

	//use for the consume json retrieve
	@GetMapping("/consumeJson")
	public User  consumeJson(){
		return userfromTopic;
	}

	// listener for string consume
	@KafkaListener(groupId = "haritest-1",topics = "haritest",containerFactory = "kafkaListenerContainerFactory" )
	public List<String> getMsgFromTopic(String data){
		messages.add(data);
		return messages;
	}

	// listener for json consume
	@KafkaListener(groupId = "haritest-2",topics = "haritest" , containerFactory = "objectKafkaListenerContainerFactory")
	public User getMsgFromTopic(User user){
		userfromTopic = user;
		return userfromTopic;
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerConsumerApplication.class, args);
	}

}
