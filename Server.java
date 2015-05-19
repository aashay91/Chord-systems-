
package util;

import org.apache.thrift.server.TServer;

import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;


public class Server {	


		public static Handler handler;
		public static FileStore.Processor processor;

		public static void main(String []args)
		{
			try { 
		        Integer.parseInt(args[0]); 
		    } catch(NumberFormatException e) { 
		         
		    	System.out.println("PORT has to be number");
		    }
			try {
				handler = new Handler(Integer.parseInt(args[0]));
				processor = new FileStore.Processor(handler);
System.out.println("prot no is "+args[0]);
				//Runnable simple = new Runnable() {
				//		public void run() {
				int port=Integer.parseInt(args[0]);
				simple(processor,port);
				//	}
				//	};      

				//new Thread(simple).start();
			} catch (Exception x) {
				x.printStackTrace();
			}
		}

		public static void simple(FileStore.Processor processor,int port) {
			try {
				TServerTransport serverTransport = new TServerSocket(port);
				TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

				System.out.println("Starting the simple server...");
				server.serve();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

