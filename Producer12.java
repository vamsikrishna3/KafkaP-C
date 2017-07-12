import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Producer12 {
	static Connection con;
	static PreparedStatement ps;
	static ResultSet rs;
	public static void main(String[] args)
	{
		if(args.length != 1)
		{
			System.err.println("please specify only one argument");
			System.exit(-1);
		}
		String topicName = args[0];
		Properties prop = new Properties();
		//Configuring Properties
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(prop);
		
		//Creating Connection
		try{
		Class.forName("com.mysql.jdbc.Driver");
		con = DriverManager.getConnection("jdbc:mysql://quickstart.cloudera:3306/test","root","cloudera");
		ps = con.prepareStatement("select * from tabs");
		rs = ps.executeQuery();
		while(rs.next()) 
		{
			String s = rs.getString(1)+" "+rs.getInt(2)+" "+rs.getFloat(3);
			System.out.println("The String Produced is "+ s);
            		ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,s);
            		producer.send(rec);
            	}
	}
		catch(SQLException e)
		{
		}
		catch(ClassNotFoundException e)
		{
		}
		finally{
		try{
        	con.close();
       		ps.close();
        	rs.close();
        	producer.close();
		} catch(Exception e)
		{
		}
		}
	}
}

