package com.zhangyw.avro.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.zhangyw.avro.action.UserCreater;
import com.zhangyw.avro.entity.User;

public class App {
	public static void main(String[] args) throws IOException {
//		List<User> users = UserCreater.createUsers(50);
//		doSerialize(users);
//		System.out.println("---end---");
		System.out.println(deSerrialize().get(10));
	}
	
	/**
	 * 序列化
	 * @throws IOException 
	 * 
	 */
	public static void doSerialize(List<User> users) throws IOException{
		DatumWriter<User> userDWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> userFileWriter = new DataFileWriter<User>(userDWriter);
		userFileWriter.create(users.get(0).getSchema(), new File("avrofile/users.avro"));
		for(User user:users){
			userFileWriter.append(user);
		}
		userFileWriter.close();
	}
	
	/**
	 * 反序列化
	 * @return
	 * @throws IOException
	 */
	public static List<User> deSerrialize() throws IOException{
		List<User> users = new ArrayList<User>();
		File file = new File("avrofile/users.avro");
		DatumReader<User> userDReader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> userReader = new DataFileReader<User>(file, userDReader);
		User user = null;
		while(userReader.hasNext()){
			user = userReader.next(user);
			users.add(user);
		}
		return users;
	}
}
