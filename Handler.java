package util;

import java.io.BufferedWriter;




import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;


import java.net.UnknownHostException;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;



import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;





public class Handler implements FileStore.Iface {
	TTransport transport;
	int port;
	String ipaddress;
	String mykey;
	NodeID pred;
	NodeID succ;
	NodeID currpred;
	static int st=0;
	static List<NodeID> finger=new ArrayList<NodeID>();
	public static Map<String,RFile>data=new HashMap<String,RFile>();
	public List<RFileMetadata>list;
	public static NodeID mypred=new NodeID();
	public static int dead_flag=0;
	public static int rmflag=0;
	Handler(int port)
	{

		File file = new File("output");
		File[] files = file.listFiles(); 
		for (File f:files) 
		{if (f.isFile() && f.exists()) 
		{ f.delete();
		}
		}
		this.port=port;
		try {
			this.ipaddress=InetAddress.getLocalHost().getHostAddress();    // ip address
			//	System.out.println(ipaddress);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mykey=sha256(ipaddress+":"+port); //my key
		//System.out.println("key:"+ mykey +" "+"ip"+" "+ipaddress+" "+"port"+port);

		/**************************SET FINGER TABLE*************************************/

		NodeID idd=new NodeID(mykey, ipaddress, port); 
		for(int i=0;i<256;i++)
		{

			finger.add(idd);

		}
		try {
			this.setNodePred(idd);
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		/*****************************************************************************/
/*
		if(port==9090)
		{
			RFile rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("temp.txt");
			rt.meta.setOwner("shrikant");
			data.put("temp.txt",rt);

			rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("tutorial.txt");
			rt.meta.setOwner("shrikant");
			data.put("tutorial.txt",rt);


			rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("tesFile.txt");
			rt.meta.setOwner("shrikant");
			data.put("tesFile.txt",rt);




			rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("test.txt");
			rt.meta.setOwner("shrikant");
			data.put("test.txt",rt);

			rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("example.txt");
			rt.meta.setOwner("shrikant");
			data.put("example.txt",rt);


			rt=new RFile();
			rt.meta=new RFileMetadata();
			rt.meta.setFilename("homework.txt");
			rt.meta.setOwner("shrikant");
			data.put("homework.txt",rt);

		}
		System.out.println(data.keySet());*/
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

	public void WriteFile(String fname,String save)
	{
		try {
			String tempfname="output/"+fname;
			File statText = new File(tempfname);
			FileOutputStream is = new FileOutputStream(statText);
			OutputStreamWriter osw = new OutputStreamWriter(is);    
			Writer w = new BufferedWriter(osw);
			w.write(save);
			w.close();
		} catch (IOException e) {

			SystemException se =new SystemException();
			se.setMessage("Problem writing to the file statsTest.txt");
			try {
				throw se;
			} catch (SystemException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}

	}


	@Override
	public void writeFile(RFile rFile) throws SystemException, TException {
		// TODO Auto-generated method stub

		int flag=1;
		int flag2=0;
		

		String nname=rFile.meta.owner;
		String n_f_name=rFile.meta.filename;
		String cont=rFile.content;
		String chkfilekey=sha256(nname+":"+n_f_name); 
		int writeflag=0;
		if(mykey.compareToIgnoreCase(mypred.getId())==0 ||( chkfilekey.compareToIgnoreCase(mykey)<=0 && chkfilekey.compareToIgnoreCase(mypred.getId())>0))
		{
			writeflag=1;
		}
		
		
		else if(mykey.compareToIgnoreCase(mypred.getId())<0)
		{
			//System.out.println(" m at exceptional case");
			if(chkfilekey.compareToIgnoreCase(mykey)<0 ||chkfilekey.compareToIgnoreCase(mypred.getId())>0)
			{
				writeflag=1;	
			}
		
			
		}
		
		if(writeflag==1)
		{


			if(true==(data.containsKey(rFile.meta.filename)))
			{
				String s_o_name=(data.get(n_f_name)).meta.owner;
				if(s_o_name.equals(nname))
				{
					rFile.meta.setUpdated(System.currentTimeMillis());

					rFile.meta.version=rFile.meta.version+1;
					flag=0;
				}
				else{
					flag=1;
					flag2=1;
					
					
				}

			}

			rFile.meta.contentLength=rFile.content.length();
			rFile.meta.contentHash=sha256(cont);
			rFile.meta.setCreated(System.currentTimeMillis());

			if(flag==1)
			{
				if(flag2==0)
				{
					rFile.meta.version=0;
					rFile.meta.setCreated(System.currentTimeMillis());
					data.put(rFile.meta.filename,rFile);
				}
				else
				{

					SystemException se=new SystemException();
					se.message="File already exist";
					throw se;

				}

			}
			//		System.out.println("version is"+rFile.meta.version);





			WriteFile(rFile.meta.filename,rFile.content);
			//System.out.println("AAdding file"+data.size());

		}
		else
		{

			SystemException se=new SystemException();
			se.message="File dosent belong to this server";
			throw se;

		}

		// TODO Auto-generated method stub

	}

	@Override
	public RFile readFile(String filename, String owner)
			throws SystemException, TException {
		// TODO Auto-generated method stub
		int flag1=0,flag2=0;
		String errmsg=null;
		String chkfilekey=sha256(owner+":"+filename);  
		
		int readflag=0;
		if(mykey.compareToIgnoreCase(mypred.getId())==0 ||( chkfilekey.compareToIgnoreCase(mykey)<=0 && chkfilekey.compareToIgnoreCase(mypred.getId())>0))
		{
			readflag=1;
		}
		
		
		else if(mykey.compareToIgnoreCase(mypred.getId())<0)
		{
			//System.out.println(" m at exceptional case");
			if(chkfilekey.compareToIgnoreCase(mykey)<0 ||chkfilekey.compareToIgnoreCase(mypred.getId())>0)
			{
				readflag=1;	
			}
		
			
		}
		
		if(readflag==1)

		{
			if(true==data.containsKey(filename))
			{
				if(owner.equals(data.get(filename).meta.owner))
				{
					if(data.get(filename).meta.getDeleted()==0)	
						return (data.get(filename));
					else
					{
						flag1=1;
						errmsg="file has been deleted";
					}

				}
				else
				{
					errmsg="ou are not right owne";
					flag1=1;
				}
			}
			SystemException se=new SystemException();
			if(flag1==1)
			{
				se.message=errmsg;
			}
			else
			{
				se.message="owner dose not exist";
			}


			throw se;
		}

		else
		{

			SystemException se=new SystemException();
			se.message="File dosent belong to this server";
			throw se;

		}
	}

	@Override
	public void deleteFile(String filename, String owner)
			throws SystemException, TException {
		// TODO Auto-generated method stub
		boolean success=false;
		String temp="outputfiles/"+filename;
		String chkfilekey=sha256(owner+":"+filename);
		int delflag=0;
		if(mykey.compareToIgnoreCase(mypred.getId())==0 ||( chkfilekey.compareToIgnoreCase(mykey)<=0 && chkfilekey.compareToIgnoreCase(mypred.getId())>0))
		{
			delflag=1;
		}
		
		
		else if(mykey.compareToIgnoreCase(mypred.getId())<0)
		{
		//	System.out.println(" m at exceptional case");
			if(chkfilekey.compareToIgnoreCase(mykey)<0 ||chkfilekey.compareToIgnoreCase(mypred.getId())>0)
			{
				delflag=1;	
			}
		
			
		}
		
		if(delflag==1)
		{
		
		if(data.containsKey(filename))
		{

			if(owner.equals(data.get(filename).meta.owner))
			{
				success = (new File  (temp)).delete();			
			}

		}
		else
		{
			SystemException se=new SystemException();
			se.message="owner dz not exist";
			throw se;
		}
		if(success)
		{

		//	System.out.println("Deleted file");
			data.get(filename).meta.setCreated(System.currentTimeMillis());

		}
		}
		else
		{
			SystemException se=new SystemException();
			se.message="File dosent belong to this server";
			throw se;

		}

	}
	/*
	@Override
	public void setFingertable(List<NodeID> node_list) throws TException {
		// TODO Auto-generated method stub
		finger=node_list;
		for(NodeID n:node_list)

		{
			System.out.println(n);
			//	finger.add(n);
		}
		mykey=sha256(ipaddress+":"+port);
		System.out.println("key:"+mykey+" "+"ip"+" "+ipaddress+" "+"port"+port);
		//System.out.println("---------------------------------");
		//System.out.println("m here "+port+"Key"+mykey);
		//System.out.println("---------------------------------");
		//System.out.println(finger);

		/*
		System.out.println("list has size"+node_list.size());
	System.out.println("m here");	
	transport = new TSocket("192.168.1.10",9091);
	transport.open();
	//System.out.println("server 9011"+share.x);
	//share.x++;

	transport.close();
	}*/

	@Override
	public NodeID findSucc(String key) throws SystemException, TException {
		// TODO Auto-generated method stub
		//Check if current node is desired node

		if(key.compareToIgnoreCase(mykey)==0)
		{
			NodeID rn=new NodeID(mykey, ipaddress, port);
			return rn;
		}
		NodeID	currpred1=findPred(key);
		//System.out.println("REturned curreny pred::"+currpred1);		
		int stt=currpred1.getPort();
		//System.out.println(stt);
		NodeID successor;
		if(currpred1.getPort()!=port)
		{
			transport = new TSocket(currpred1.getIp(),currpred1.getPort());

			transport.open();
			FileStore.Client client;
			TProtocol protocol = new  TBinaryProtocol(transport);
			client = new FileStore.Client(protocol);
			successor=client.getNodeSucc();
			transport.close();
		}
		else
		{
			if(dead_flag==1)
			{
				successor=this.getNodeSucc();
				dead_flag=0;

			}
			else
				successor=currpred1;
		}
	//	System.out.println("final succ"+successor.getPort());
		return successor;


		/*
		findPred(key);

		System.out.println(node_list);
		NodeID n=node_list.get(0);
		String s;
		s=node_list.get(0).id;
		int s1=node_list.get(0).port;
		System.out.println("id"+s+"port"+s1);
		share.x++;
		String ss="335cb3c79fb012e06cbcc38047251cfe41b710e83d45b8660b1bfc243bed93e7";
		if(s.compareToIgnoreCase(ss)==0)
		{
			System.out.println("mathc");
		}

		if(s1==9092)
		{
			System.out.println("serve 9092"+share.x);

		}*/

	}

	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		// TODO Auto-generated method stub
		int i;
		NodeID nd;

		for( i=0;i<finger.size();i++ )
		{

			nd=finger.get(i);
			NodeID nd2;
			//Check first finger maches to search
			if(mykey.compareToIgnoreCase(nd.getId())==0)
			{
				currpred=new NodeID(mykey, ipaddress, port);

				return currpred;
			}

			if(i==0&&nd.getId().equalsIgnoreCase(key))
			{
				pred=nd;
				currpred=new NodeID(mykey, ipaddress, port);
					//System.out.println("I found match");
				return currpred;
			}
			else if(mykey.compareToIgnoreCase(nd.getId())>=1)
			{
				if(i==0&&(key.compareToIgnoreCase(mykey)>0&&key.compareToIgnoreCase(nd.getId())>0))
				{
					dead_flag=1;
					pred=nd;
					currpred=new NodeID(mykey, ipaddress, port);
	//			System.out.println("I found match at end of ring(for larger)");
					return currpred;
				}
				else if(i==0&&(key.compareToIgnoreCase(mykey)<0&&key.compareToIgnoreCase(nd.getId())<0))
				{
					dead_flag=1;
					pred=nd;
					currpred=new NodeID(mykey, ipaddress, port);
//					System.out.println("I found match at end of ring(for smaller)");
					return currpred;
				}
			}

			else if(i==0)

			{
				//Range
				if(key.compareToIgnoreCase(mykey)>0 &&key.compareToIgnoreCase(nd.getId())<0)
				{
					/*
					transport = new TSocket(nd.getIp(),nd.getPort());

					transport.open();
					FileStore.Client client;
					TProtocol protocol = new  TBinaryProtocol(transport);
					client = new FileStore.Client(protocol);
System.out.println("call to now"+nd.getPort());
					return client.findPred(key);*/

				//	System.out.println("ownkey and first finger");
					dead_flag=1;
					pred=nd;
					currpred=new NodeID(mykey, ipaddress, port);
					return currpred;
					//return pred;
				}
				/*
				//fst is > sec when i=0
				if(key.compareToIgnoreCase(mykey)<=-1 && key.compareToIgnoreCase(nd.getId())<=-1)
				{
					System.out.println("ownkey and first finger but owner is bigger");		

					pred=nd;
					currpred=new NodeID(mykey, ipaddress, port);
					return currpred;
					//return pred;

				}
				 */

			}

			if(i<finger.size()-1)
			{
				nd2=finger.get(i+1);
				//Normal condition

				if(nd.getId().compareToIgnoreCase(nd2.getId())>=1)
				{
					if(key.compareToIgnoreCase(nd.getId())<0 && nd.port!=port &&(key.compareToIgnoreCase(nd2.getId())<0 ||key.compareToIgnoreCase(nd2.getId())==0))
					{

					//	System.out.println("fst is greater than second  condi");
						transport = new TSocket(nd.getIp(),nd.getPort());

						transport.open();
						FileStore.Client client;
						TProtocol protocol = new  TBinaryProtocol(transport);
						client = new FileStore.Client(protocol);
			//			System.out.println("call to now(larger node)"+nd.getPort());
						NodeID temp =client.findPred(key);
						transport.close();
						return temp;
					}
					else if(key.compareToIgnoreCase(nd.getId())>0 && nd.port!=port&& (key.compareToIgnoreCase(nd2.getId())>0 ||key.compareToIgnoreCase(nd2.getId())==0))
					{
				//		System.out.println("fst is greater than second then rev condi");
						transport = new TSocket(nd.getIp(),nd.getPort());

						transport.open();
						FileStore.Client client;
						TProtocol protocol = new  TBinaryProtocol(transport);
						client = new FileStore.Client(protocol);
					//	System.out.println("call to now(first bigger"+nd.getPort());
						NodeID temp =client.findPred(key);
						transport.close();
						return temp;	
					}
				}

				if(key.compareToIgnoreCase(nd.getId())>0 && nd.port!=port &&(key.compareToIgnoreCase(nd2.getId())<0||key.compareToIgnoreCase(nd2.getId())==0))
				{


				//System.out.println("normal conditon ");
					transport = new TSocket(nd.getIp(),nd.getPort());

					transport.open();
					FileStore.Client client;
					TProtocol protocol = new  TBinaryProtocol(transport);
					client = new FileStore.Client(protocol);
			//		System.out.println("Normal:call to now"+nd.getPort());
					NodeID temp=client.findPred(key);
					transport.close();
					return temp;

				}



			}


			//System.out.println("NO condi math move ahead:M at"+i);


		}//for

		//No match found or no interval then last node
		System.out.println("last index"+i);
		if(i==finger.size())
		{
			nd=finger.get(i-1);
			if(nd.getId().compareToIgnoreCase(mykey)!=0)
			{
		//		System.out.println("End finger now conneccting to"+nd.getPort());
				transport = new TSocket(nd.getIp(),nd.getPort());

				transport.open();
				FileStore.Client client;
				TProtocol protocol = new  TBinaryProtocol(transport);
				client = new FileStore.Client(protocol);
				NodeID temp=client.findPred(key);
				transport.close();
				return temp;
			}
			else
			{
				//System.out.println("node not exist");
				dead_flag=0;
				return nd;
				/*
				TIOStreamTransport prt=new  TIOStreamTransport(System.out);
				TProtocol protocol1 = new TJSONProtocol.Factory().getProtocol(prt);
				SystemException se =new SystemException();
				se.setMessage("Node dznt exist");
				throw se;*/
			}


		}

		return null;
	}

	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		// TODO Auto-generated method stub
		//System.out.println(" hello m at here");
		return finger.get(0);

	}
	@Override
	public void setNodePred(NodeID nodeId) throws SystemException, TException {
		// TODO Auto-generated method stub
		mypred=nodeId;
	//	System.out.println("My pred is:"+mypred);
	}
	@Override
	public void updateFinger(int idx, NodeID nodeId) throws SystemException,
	TException {
		// TODO Auto-generated method stubis
		finger.set(idx, nodeId);



//		System.out.println("Added/removed at index"+idx+"->"+nodeId.getPort());

	}
	@Override
	public List<NodeID> getFingertable() throws SystemException, TException {
		// TODO Auto-generated method stub
		return finger;
	}
	@Override
	public List<RFile> pullUnownedFiles() throws SystemException, TException {
		// TODO Auto-generated method stub
//		System.out.println("<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>");
		String pradkey=mypred.getId();
		List<RFile> files=new ArrayList<RFile>();
		Set<String>k_eys=data.keySet();
		List<String>storekey=new ArrayList<String>();
		int size=data.size();
	//	System.out.println("---------------------------");
		System.out.println(data.keySet());
		//System.out.println("<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>");
		if(mykey.compareToIgnoreCase(pradkey)<0)
		{
		//	System.out.println("REverse case");
			for(String k:k_eys)
			{
				String chk_key=sha256(data.get(k).meta.getOwner()+":"+data.get(k).meta.getFilename());
				if(chk_key.compareToIgnoreCase(mykey)>0 && chk_key.compareToIgnoreCase(pradkey)<=0 )
				{

					files.add(data.get(k));
					storekey.add(k);
				}

			}

		}		
		else
		{
		//	System.out.println("nORMAL CASE");
			for(String k:k_eys)
			{
				String chk_key=sha256(data.get(k).meta.getOwner()+":"+data.get(k).meta.getFilename());
				if(chk_key.compareToIgnoreCase(mykey)<0 && chk_key.compareToIgnoreCase(pradkey)>=0 )
				{
					files.add(data.get(k));
					storekey.add(k);
				}

			}


		}

		if(storekey.size()!=0)
		{
			for(String a:storekey)
			{
				data.remove(a);
			}
		}
	//	System.out.println("m returning files"+files);
	//	System.out.println("my size is"+data.size());
		return files;
	}
	@Override
	public void pushUnownedFiles(List<RFile> files) throws SystemException,
	TException {
		// TODO Auto-generated method stub



		for(RFile r:files)
		{
			data.put(r.meta.getFilename(),r);
		}

	//	System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$444");
		//System.out.println(data); 

		//System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

	}



	@Override
	public void join(NodeID nodeId) throws SystemException, TException {
		// TODO Auto-generated method stub
		if(nodeId==null)
		{
			NodeID idd=new NodeID(mykey, ipaddress, port); 
			for(int i=0;i<256;i++)
			{

				finger.add(idd);

			}
			//System.out.println("added"+idd);
			this.setNodePred(idd);
		}
		else
		{
			/*
			String str=ipaddress+":"+port;
			BigInteger p=new BigInteger(str.getBytes());
			for(int i=1;i<=256;i++)
			{
				BigInteger tw=	(BigInteger.valueOf((long) Math.pow(2, i-1)));

				//BigInteger bitw=new BigInteger(tw.toString());
				p=p.add(tw);
				String st6=new String(p.toByteArray());
				System.out.println(sha256(st6));
				//System.out.println("key"+i+st6);
				//finger.add(e)

				p=new BigInteger(str.getBytes());

			}*/


			BigInteger p=new BigInteger(mykey,16);
			for(int i=1;i<=256;i++)
			{
				BigInteger tw1=	BigInteger.valueOf(2);
				BigInteger tw=tw1.pow(i-1);
				//BigInteger bitw=new BigInteger(tw.toString());
			//	System.out.println(tw);
				p=p.add(tw);
				String st6=new String(p.toString(16));

				//System.out.println("key"+i+st6);
				transport = new TSocket(nodeId.getIp(),nodeId.getPort());
				transport.open();
				FileStore.Client client;
				TProtocol protocol = new  TBinaryProtocol(transport);
				client = new FileStore.Client(protocol);
				NodeID temp=client.findSucc(st6);
				
				finger.set(i-1,temp);
			//	System.out.println("port->"+i+temp.getPort());
				transport.close();



				p=new BigInteger(mykey,16);

			}
		//	System.out.println("---Finger-----");
			for(NodeID n:finger)

			{
			//	System.out.println(n.getPort());
				//	finger.add(n);
			}
			this.setNodePred(this.findPred(mykey));

			/****************Change my succ pred*******************/
			NodeID succ_pred=this.getNodeSucc();
			TTransport transport1 = new TSocket(succ_pred.getIp(),succ_pred.getPort());
			transport1.open();
			FileStore.Client client1;
			TProtocol protocol1 = new  TBinaryProtocol(transport1);
			client1 = new FileStore.Client(protocol1);
			client1.setNodePred(new NodeID(mykey, ipaddress, port));
			transport1.close();
			/********************************************************/

		//	System.out.println("--------My pred is< "+this.port+"-->"+mypred+">-------");
		//	System.out.println("---Finger-----");
			for(NodeID n:finger)

			{
			//	System.out.println(n.getPort());
				//	finger.add(n);
			}

			/**************Finger table ready************/
			Boolean change=false;
			BigInteger tw1=	BigInteger.valueOf(2);
			BigInteger tw;
			NodeID pred=null;
			BigInteger p_node;
			List<NodeID> finger2=new ArrayList<NodeID>();
			BigInteger maxval=tw1.pow(256);
			for(int i=1;i<=256;i++)
			{
				p=new BigInteger(mykey,16);
				tw=tw1.pow(i-1);
				p=p.subtract(tw);
				if(p.signum()==-1)
				{
					p.add(maxval);
				}
				String key=new String(p.toString(16));
				String calkey=key;
				change=false;
				do{
					NodeID xx=this.findPred(key);
					if(pred==xx)
					{
				//		System.out.println("circualr");
						change=false;
						break;
					}
					pred=xx;

					if(mykey.compareToIgnoreCase(pred.getId())!=0)
					{
						transport = new TSocket(pred.ip,pred.port);
						transport.open();
						FileStore.Client client;
						TProtocol protocol = new  TBinaryProtocol(transport);
						client = new FileStore.Client(protocol);

						if(client.getNodeSucc().getId().compareToIgnoreCase(calkey)==0)
						{
							//System.out.println("m giving succ of cal pred");

							pred=client.getNodeSucc();
							transport.close();
							transport = new TSocket(pred.ip,pred.port);
							transport.open();
							protocol = new  TBinaryProtocol(transport);
							client = new FileStore.Client(protocol);


						}


						finger2=client.getFingertable();

						p_node= new BigInteger (pred.getId(),16);
						p_node=p_node.add(tw);




						String st6=new String(p_node.toString(16));
						if(mykey.compareToIgnoreCase(mypred.getId())<0)
						{
							change=false;
						//	System.out.println("------------exceptional condi-------------");
							if(st6.compareToIgnoreCase(mykey)<=0)
							{
								//System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
								change=true;
								client.updateFinger(i-1,  new NodeID(mykey, ipaddress, port));
								//System.out.println("----->>>>>>>Exceptionla updated ALL SMALL"+pred.getPort());
								key=pred.getId();

							}
							else if(st6.compareToIgnoreCase(mypred.getId())>=0)
							{
							//	System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
								change=true;
								client.updateFinger(i-1,  new NodeID(mykey, ipaddress, port));
								//System.out.println("----->>>>>>>Exceptionla updated ALL bIG"+pred.getPort());
								key=pred.getId();

							}

						}
						else if(st6.compareToIgnoreCase(mykey)<=0 && st6.compareToIgnoreCase(mypred.getId())>=1)
						{
						//	System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
							change=true;
							client.updateFinger(i-1, new NodeID(mykey, ipaddress, port));
							key=pred.getId();

						}
						else
						{
							change=false;
						}



					}

					else
						change=false;
					transport.close();




				}while(change);
			}//for

			/*/*********************************Pullfiles from succ*******************/
			NodeID succ_pred1=this.getNodeSucc();
		//	System.out.println("My file succ is "+succ_pred1.getPort());
			TTransport transport11 = new TSocket(succ_pred1.getIp(),succ_pred1.getPort());
			transport11.open();
			FileStore.Client client11;
			TProtocol protocol11 = new  TBinaryProtocol(transport11);
			client11 = new FileStore.Client(protocol11);
			List<RFile>temprf=null;
			System.out.println("Calling pull");
			temprf=client11.pullUnownedFiles();

			if(temprf.size()!=0)
			{
				for(RFile r:temprf)
				{

					String user=r.meta.getOwner();
					String filenm=r.meta.getFilename();
					//r.meta.setCreated(System.currentTimeMillis()); //check change of creted time
					this.data.put(filenm, r);
					//shoudl i call write function
					System.out.println("Added"+filenm);
				}
			}
			else
				System.out.println("NO file found");
			transport11.close();
			/***********************************************************************/
		}//if
	//	System.out.println("/////////////////////////////////");
	//	System.out.println("updated size of hash map"+data.size()); 

		//System.out.println("/////////////////////////////////");

		/*
		for(NodeID n:finger)

		{
			System.out.println(n);
			//	finger.add(n);
		}*/

	}
	@Override
	public void remove() throws SystemException, TException {
		// TODO Auto-generated method stub
		rmflag=1;






		BigInteger p;
	//	System.out.println("--------------------------------m gettiing removed-----------------------------------------");
		Boolean change=false;
		BigInteger tw1=	BigInteger.valueOf(2);
		BigInteger tw;
		NodeID pred=null;
		BigInteger p_node;
		List<NodeID> finger2=new ArrayList<NodeID>();
		BigInteger maxval=tw1.pow(256);
		for(int i=1;i<=256;i++)
		{
			p=new BigInteger(mykey,16);
			tw=tw1.pow(i-1);
			p=p.subtract(tw);
			if(p.signum()==-1)
			{
				p.add(maxval);
			}
			String key=new String(p.toString(16));
			String calkey=key;
			change=false;
	//		System.out.println("++++++++++++++++++My pred is:-"+"<"+mypred+">");
			do{

				NodeID xx=this.findPred(key);
	//			System.out.println("------------------------------>pred found:"+xx.port);
				if(pred==xx)
				{
		//			System.out.println("circualr");
					change=false;


					break;
				}
				pred=xx;

				if(mykey.compareToIgnoreCase(pred.getId())!=0)
				{
					transport = new TSocket(pred.ip,pred.port);
					transport.open();
					FileStore.Client client;
					TProtocol protocol = new  TBinaryProtocol(transport);
					client = new FileStore.Client(protocol);

					if(client.getNodeSucc().getId().compareToIgnoreCase(calkey)==0)
					{
				//		System.out.println("m giving succ of cal pred");

						pred=client.getNodeSucc();
						transport.close();
						transport = new TSocket(pred.ip,pred.port);
						transport.open();
						protocol = new  TBinaryProtocol(transport);
						client = new FileStore.Client(protocol);


					}


					finger2=client.getFingertable();

					p_node= new BigInteger (pred.getId(),16);
					p_node=p_node.add(tw);
					String st6=new String(p_node.toString(16));

					if(mykey.compareToIgnoreCase(mypred.getId())<0)
					{
						change=false;
			//			System.out.println("------------exceptional condi-------------");
						if(st6.compareToIgnoreCase(mykey)<=0)
						{
				//			System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
							change=true;
							client.updateFinger(i-1, this.getNodeSucc());
					//		System.out.println("----->>>>>>>Exceptionla updated ALL SMALL"+pred.getPort());
							key=pred.getId();

						}
						else if(st6.compareToIgnoreCase(mypred.getId())>=0)
						{
						//	System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
							change=true;
							client.updateFinger(i-1, this.getNodeSucc());
							//System.out.println("----->>>>>>>Exceptionla updated ALL bIG"+pred.getPort());
							key=pred.getId();

						}

					}
					else if(st6.compareToIgnoreCase(mykey)<=0 && st6.compareToIgnoreCase(mypred.getId())>=1)
					{
					//	System.out.println("For key:"+key+"->"+"Pred-"+pred.getPort());
						change=true;
						client.updateFinger(i-1, this.getNodeSucc());
				//		System.out.println("-------------------------->find pred of "+pred.getPort());
						key=pred.getId();

					}
					else
					{
						change=false;
					}


					transport.close();
				}

				else
					change=false;






			}while(change);



		}

		/*************************PUSH FILES TO SUCC****************/


		NodeID succ_pred1=this.getNodeSucc();
		TTransport transport12 = new TSocket(succ_pred1.getIp(),succ_pred1.getPort());
		transport12.open();
		FileStore.Client client12;
		TProtocol protocol12 = new  TBinaryProtocol(transport12);
		client12 = new FileStore.Client(protocol12);
		List<RFile>rx=new ArrayList<RFile>();
		for(RFile r:data.values())
		{
			rx.add(r);
		}


		client12.pushUnownedFiles(rx);
		transport12.close();		





		/*************************************************************/


		/***********Change my succ pre to my pred ****************/


		NodeID succ_pred=this.getNodeSucc();
		TTransport transport1 = new TSocket(succ_pred.getIp(),succ_pred.getPort());
		transport1.open();
		FileStore.Client client1;
		TProtocol protocol1 = new  TBinaryProtocol(transport1);
		client1 = new FileStore.Client(protocol1);
		client1.setNodePred(this.mypred);
		transport1.close();
	//	System.out.println("changed my succ to pred to"+mypred);



		/*********************************************************/


	}

}
