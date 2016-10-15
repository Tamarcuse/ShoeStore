package tests;

import static org.junit.Assert.*;

import bgu.spl.mics.Message;
import bgu.spl.mics.MessageBusImpl;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import bgu.spl.mics.MessageBusImpl;
import bgu.spl.mics.MicroService;

public class MicroServiceTests {

	@Test
	public void test() {
		MessageBusImpl bus = MessageBusImpl.getInstance();
		ConcurrentHashMap<String , ConcurrentLinkedQueue<Message>> allQueuesTest = new ConcurrentHashMap<>();              // Mapping from a microService name to it's message-queue
		allQueuesTest.put("A", new ConcurrentLinkedQueue<Message>());
		allQueuesTest.put("B", new ConcurrentLinkedQueue<Message>());
		allQueuesTest.put("C", new ConcurrentLinkedQueue<Message>());
		System.out.println(bus.roundRobin(allQueuesTest));


	}

}
