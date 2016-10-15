package bgu.spl.mics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class MessageBusImpl implements MessageBus {
	
	// ***********  Singleton **************	
	
		private static class SingletonHolder{
			private static MessageBusImpl instance = new MessageBusImpl();
		}
		
		private MessageBusImpl() {}
		
		public static MessageBusImpl getInstance(){
			return SingletonHolder.instance;
		}
	// ***********  /Singleton **************	
	
	private ConcurrentHashMap<MicroService , ConcurrentLinkedQueue<Message>> allQueues = new ConcurrentHashMap<>();                          // Mapping from a microService name to it's message-queue
	// Every access to a MicroService queue goes through allQueues!!
	
	private ConcurrentHashMap<Class <? extends Request>, ConcurrentLinkedQueue<MicroService>> request2Subs = new ConcurrentHashMap<>();     // Mapping from a Request type to a queue of the MicroServices'-queues which are subscribed to it
	private ConcurrentHashMap<Class <? extends Broadcast>, ConcurrentLinkedQueue<MicroService>> broadcast2Subs = new ConcurrentHashMap<>(); // Mapping from a Broadcast type to a queue of the MicroServices'-queues which are subscribed to it
	private ConcurrentHashMap<MicroService, ConcurrentLinkedQueue<Class <? extends Request>>> sub2Requests = new ConcurrentHashMap<>();     // Mapping from a MicroService to requests it is subscribed to
	private ConcurrentHashMap<MicroService, ConcurrentLinkedQueue<Class <? extends Broadcast>>> sub2Broadcasts = new ConcurrentHashMap<>(); // Mapping from a MicroService to requests it is subscribed to
	private ConcurrentHashMap<Class <? extends Request>, MicroService> req2Requester = new ConcurrentHashMap<>();          // Mapping from a Request message to the queue of the the requesting MicroService
	
	private void addToSub2Broadcasts(MicroService ms, Class <? extends Broadcast> brdCstType){
		if(brdCstType != null){
			sub2Broadcasts.putIfAbsent(ms, new ConcurrentLinkedQueue<>());
			sub2Broadcasts.get(ms).add(brdCstType);
		}
	}
	
	private void addToreq2Requester(Class <? extends Request> reqType , MicroService ms){
		if(ms != null){
			req2Requester.put(reqType, ms);
		}
	}
	
	private void addToSub2Requests(MicroService ms, Class <? extends Request> reqType){
		if(reqType != null){
			sub2Requests.putIfAbsent(ms, new ConcurrentLinkedQueue<>());
			sub2Requests.get(ms).add(reqType);
		}
	}
	
	private void addToMsQueue(MicroService ms , Message msg) {
		if(msg != null && allQueues.containsKey(ms)){
			allQueues.get(ms).add(msg);
		}
		else{
			System.out.println("No such MicroService or Message is null");
		}
	}
	
	private void addSubToReq2Subs(Class <? extends Request> req ,MicroService sub){
		if(sub != null){
			request2Subs.putIfAbsent(req, new ConcurrentLinkedQueue<>());
			request2Subs.get(req).add(sub);
		}
	}
	
	private void addToBroadcast2Subs(Class <? extends Broadcast> brdcst ,MicroService sub){
		if(sub != null){
			broadcast2Subs.putIfAbsent(brdcst, new ConcurrentLinkedQueue<>());
			broadcast2Subs.get(brdcst).add(sub);
		}
	}
	
	
	@Override // make MicroService @m a subscriber of Request @type
	public void subscribeRequest(Class<? extends Request> type, MicroService m) {
		addSubToReq2Subs(type, m);
		addToSub2Requests(m, type);
	}

	@Override
	public void subscribeBroadcast(Class<? extends Broadcast> type, MicroService m) {
		addToBroadcast2Subs(type, m);
		addToSub2Broadcasts(m, type);
	}

	@Override // creating the RequestCompleted Message and adding it to the Requester's queue
	
	public <T> void complete(Request<T> r, T result) {
		RequestCompleted<T> rCompleted = new RequestCompleted<T>(r, result);
		addToMsQueue(req2Requester.get(r), rCompleted);
	}

	@Override
	public void sendBroadcast(Broadcast b) {
		ConcurrentLinkedQueue<MicroService> bList = broadcast2Subs.get(b);
		Iterator<MicroService> it = bList.iterator();
		while(it.hasNext()){
			addToMsQueue(it.next(), b);
		}		
	}

	@Override
	// We poll a subscriber out of the DS and use it and add it back to the end of the DS
	// thus creating a round robin way of work and that's why the method is synchronized
	public synchronized boolean sendRequest(Request<?> r, MicroService requester) {
		MicroService sub = request2Subs.get(r).poll();
		if(sub != null){
			addToMsQueue(sub, r);
			addToreq2Requester(r.getClass(), requester);
			addSubToReq2Subs(r.getClass(), sub);
			return true;
		}
		return false;
	}



	@Override
	public void register(MicroService m) {
		if(m != null){
			allQueues.putIfAbsent(m, new ConcurrentLinkedQueue<>());	
		}
	}

	@Override
	public void unregister(MicroService m) {
		Class<? extends Request> key;
		allQueues.remove(m);
		// Remove the MicroService from req2Requester:
		Set<Entry<Class<? extends Request>, MicroService>> set = req2Requester.entrySet();
		Iterator<Entry<Class<? extends Request>, MicroService>> it = set.iterator();
		while(it.hasNext()){
			if(it.next().getValue().equals(m)){
				key = it.next().getKey();
				req2Requester.remove(key);
			}
		}
	}

	@Override
	public Message awaitMessage(MicroService m) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}


}
