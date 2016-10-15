package bgu.spl.mics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.Iterator;
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
	private ConcurrentHashMap<Class <? extends Request>, ConcurrentLinkedQueue<MicroService>> requestToSubs = new ConcurrentHashMap<>();     // Mapping from a Request type to a queue of the MicroServices'-queues which are subscribed to it
	private ConcurrentHashMap<Class <? extends Broadcast>, ConcurrentLinkedQueue<MicroService>> broadcastToSubs = new ConcurrentHashMap<>(); // Mapping from a Broadcast type to a queue of the MicroServices'-queues which are subscribed to it
	private ConcurrentHashMap<MicroService, ConcurrentLinkedQueue<Class <? extends Request>>> subToRequests = new ConcurrentHashMap<>();     // Mapping from a MicroService to requests it is subscribed to
	private ConcurrentHashMap<MicroService, ConcurrentLinkedQueue<Class <? extends Broadcast>>> subToBroadcasts = new ConcurrentHashMap<>(); // Mapping from a MicroService to requests it is subscribed to
	private ConcurrentHashMap<Class <? extends Request>, ConcurrentLinkedQueue<Message>> req2Requester = new ConcurrentHashMap<>();          // Mapping from a Request message to the queue of the the requesting MicroService
	
	private void addToSub2Broadcasts(MicroService ms, Class <? extends Broadcast> brdCstType){
		if(brdCstType != null){
			subToBroadcasts.putIfAbsent(ms, new ConcurrentLinkedQueue<>());
			subToBroadcasts.get(ms).add(brdCstType);
		}
	}
	
	private void addToreq2Requester(Class <? extends Request> reqType , Message msg){
		if(msg != null){
			req2Requester.putIfAbsent(reqType, new ConcurrentLinkedQueue<>());
			req2Requester.get(reqType).add(msg);
		}
	}
	
	private void addToSub2Requests(MicroService ms, Class <? extends Request> reqType){
		if(reqType != null){
			subToRequests.putIfAbsent(ms, new ConcurrentLinkedQueue<>());
			subToRequests.get(ms).add(reqType);
		}
	}
	
	private void addToMsQueue(MicroService ms , Message msg) {
		if(msg != null && allQueues.containsKey(ms)){
			allQueues.get(ms).add(msg);
		}
	}
	
	private void addSubToReq2Subs(Class <? extends Request> req ,MicroService sub){
		if(sub != null){
			requestToSubs.putIfAbsent(req, new ConcurrentLinkedQueue<>());
			requestToSubs.get(req).add(sub);
		}
	}
	
	private void addToBroadcast2Subs(Class <? extends Broadcast> brdcst ,MicroService sub){
		if(sub != null){
			broadcastToSubs.putIfAbsent(brdcst, new ConcurrentLinkedQueue<>());
			broadcastToSubs.get(brdcst).add(sub);
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
		req2Requester.get(r).add(rCompleted);
	}

	@Override
	public void sendBroadcast(Broadcast b) {
		ConcurrentLinkedQueue<MicroService> bList = broadcastToSubs.get(b);
		Iterator<MicroService> it = bList.iterator();
		while(it.hasNext()){
			addToMsQueue(it.next(), b);
		}		
	}

	@Override
	// We poll a subscriber out of the DS and use it and add it back to the end of the DS
	// thus creating a round robin way of work and that's why the method is synchronized
	public synchronized boolean sendRequest(Request<?> r, MicroService requester) {
		MicroService sub = requestToSubs.get(r).poll();
		if(sub != null){
			addToMsQueue(sub, r);
			req2Requester.put(r.getClass(), allQueues.get(requester));
			requestToSubs.get(r).add(sub);
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
		allQueues.remove(m);
		Set<? extends Request> set = req2Requester.entrySet()
		
	}

	@Override
	public Message awaitMessage(MicroService m) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}


}
