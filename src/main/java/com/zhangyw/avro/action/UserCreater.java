package com.zhangyw.avro.action;

import java.util.ArrayList;
import java.util.List;

import com.zhangyw.avro.entity.User;

public class UserCreater {
	public static List<User> createUsers(int count){
		List<User> users = new ArrayList<User>();
		for(int i = 0;i<count;i++){
			User user = User.newBuilder().setUsername("username"+i).setPassword("000000").setSex(i%2).setAge(20).build();
			users.add(user);
		}
		return users;
	}
}
