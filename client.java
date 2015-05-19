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






public class client {

	/**
	 * @param args
	 */

	public static String user;
	public static String host;
	public static String op;
	public static String infile;
	public static int por;
	public static String fullpath;
	public static String ss[]=new String[100];
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println();
		if(args.length==8)
		{
			FileStore.Client client;	
			TTransport transport;
			TIOStreamTransport prt=new  TIOStreamTransport(System.out);
			TProtocol protocol1 = new TJSONProtocol.Factory().getProtocol(prt);

			try {
				setinput(args,args.length);
			} catch (SystemException e2) {
				/*
				try {

					//e2.write(protocol1);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
			} catch (TException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			//System.out.println("m at first call"+host+por);


			//First RPC CALL
			transport = new TSocket(host,por);

			try {
				transport.open();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//System.out.println("file name become"+infile);
			String sh=sha256(user+":"+infile);
			//System.out.println("key"+sh);

			
			TProtocol protocol = new  TBinaryProtocol(transport);
			client = new FileStore.Client(protocol);
		//	System.out.println("key"+sh);
		
			try {
				//KEY------>SHA-256 OWNER AND FILE NAME
				NodeID md=(client.findSucc(sh));
		//System.out.println("succ found"+md.port);
				transport.close();



				//SECOND RPC CALL    
				transport = new TSocket(md.getIp(),md.getPort());

				try {
					transport.open();
				} catch (TTransportException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		


				protocol = new  TBinaryProtocol(transport);
				client = new FileStore.Client(protocol);
		//		System.out.println(user+op+infile);
				perform(user,op,infile,client);

			} catch (SystemException e) {
				// TODO Auto-generated catch block

				System.out.println(e.getMessage());
			} catch (TException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
			}
		}
		else
		{
			System.out.println("wrong no if inputs");
		}
	}

	private static void setinput(String arg[],int count)throws TException,SystemException

	{
		host=arg[0];
		por=Integer.parseInt(arg[1]);
		int cnt=count-2;
		int i=2;

		try { 
			Integer.parseInt(arg[1]); 
		} catch(NumberFormatException e) { 


			System.out.println("Port no is invalid");


		}
		// 

		while(i<=cnt)
		{
			if(true==arg[i].equals("--operation"))
			{
				i++;
				op=arg[i];
				i++;
			}
			else if(true==arg[i].equals("--filename"))
			{
				i++;
				fullpath=arg[i];

				ss=arg[i].split("/");
				int x;
				x=ss.length;
				infile=ss[x-1];
				i++;
			}
			else if(true==arg[i].equals("--user"))
			{
				i++;

				user=arg[i];
				i++;
			}


			else{
				System.out.println("Enter proper input");
				System.exit(0);


			}
		}

	}

	private static void perform(String owner,String operation,String fname,FileStore.Client client) throws TException,SystemException
	{
		RFile rf=new RFile();
		TIOStreamTransport prt=new  TIOStreamTransport(System.out);
		TProtocol protocol = new TJSONProtocol.Factory().getProtocol(prt);
		StringBuilder bd=new StringBuilder();
		String line=null;
		if(operation.equals("write"))
		{	
			FileReader rd=null;
			rf.meta=new RFileMetadata();	
			try {

	//	System.out.println("file poath is"+fullpath);
				rd=new FileReader(fullpath);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				SystemException se =new SystemException();
				se.setMessage("input file dosen't exist");
				throw se;
			}

			BufferedReader br=new BufferedReader(rd);
			try {
				line=br.readLine();
				while(line!=null)
				{
					bd.append(line);
					bd.append("\n");
					line=br.readLine();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				SystemException se =new SystemException();
				se.setMessage("Exception occured while reading input file");
				throw se;
			}

	//		System.out.println("m writting file"+fname);
			rf.meta.setFilename(fname);
			rf.content=bd.toString();	
			rf.meta.setOwner(owner);
			client.writeFile(rf);

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				SystemException se =new SystemException();
				se.setMessage("Exception occured while closing file");
				throw se;
			}
		}

		else if(operation.equals("read"))
		{


			RFile r=null;
			r= client.readFile(fname,owner);
			r.write(protocol);

			//r.write(protocol);
		}

		else if(operation.equals("delete"))
		{
			client.deleteFile(fname, owner);

		}

		else{

			System.out.println("Enter proper operation");
			System.exit(0);

		}

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
