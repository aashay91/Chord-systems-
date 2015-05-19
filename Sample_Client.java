package util;
import java.io.BufferedReader;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Sample_Client {

	/**
	 * @param args
	 * @throws TException 
	 * @throws SystemException 
	 */
	public static void main(String[] args) throws SystemException, TException {
		// TODO Auto-generated method stub
		FileStore.Client client;	
		TTransport transport;
		TIOStreamTransport prt=new  TIOStreamTransport(System.out);
		TProtocol protocol1 = new TJSONProtocol.Factory().getProtocol(prt);
		
		transport = new TSocket("127.0.1.1",9092);
		String sh=sha256("127.0.1.1"+":"+9090);
		
		try {
			transport.open();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	System.out.println("m connecitng ot 91");
		TProtocol protocol = new  TBinaryProtocol(transport);
		client = new FileStore.Client(protocol);
	
	NodeID nd=new NodeID(sh,"127.0.1.1",9090);
     //NodeID nd=null;
			//KEY------>SHA-256 OWNER AND FILE NAME
client.join(nd);

	//client.remove();
		transport.close();
		//System.out.println("M now remving");
		//client.remove();
			//transport.close();
		
	}
	
	public static String sha256(String base) {
		try{
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(base.getBytes());
			StringBuffer hexString = new StringBuffer();

			for (int i = 0; i < hash.length; i++) {
				String hex = Integer.toHexString(0xff & hash[i]);
				if(hex.length() == 1) hexString.append('0');
				hexString.append(hex);
			}

			return hexString.toString();
		} catch(Exception ex){
			throw new RuntimeException(ex);
		}
	}


}
