package com.mangocity.zoo.demo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class FirstZK {

	private static final String ZK_HOST = "182.61.33.175";

	private static final String ZK_PORT = "2181";

	private static final String ZK_CONNECTION_CLUSTER = "10.10.152.107:21811,10.10.152.107:21812,10.10.152.107:21813";

	private static int TIME_OUT = 3000;// 毫秒

	public static void main(String[] args) throws Exception {
		// firstZK();
		testCreate();
	}

	private static void testCreate() throws Exception {
		ZooKeeper zk = new ZooKeeper(ZK_CONNECTION_CLUSTER, TIME_OUT, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("Connection is Builder...getPath(): " + event.getPath() + " ,getState(): "
						+ event.getState() + " ,getType: " + event.getType());
			}
		});
		//zk.create("/Lock",null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create("/Lock/lock_seq_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		List<String> childrenList = zk.getChildren("/Lock", true);
		System.out.println("Lock　List: " + childrenList);
		// 关闭连接
		zk.close();
	}
	
	// 测试配置管理
	private static void testConfigManagement() throws Exception {
		ZooKeeper zk = new ZooKeeper(ZK_CONNECTION_CLUSTER, TIME_OUT, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("Connection is Builder...getPath(): " + event.getPath() + " ,getState(): "
						+ event.getState() + " ,getType: " + event.getType());
			}
		});
		zk.create("/Configuration", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		TimeUnit.SECONDS.sleep(3);
		zk.delete("/testCluster", -1);
		// 关闭连接
		zk.close();
	}

	// 测试集群
	private static void testCluster() throws Exception {
		ZooKeeper zk = new ZooKeeper(ZK_CONNECTION_CLUSTER, TIME_OUT, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("Connection is Builder...getPath(): " + event.getPath() + " ,getState(): "
						+ event.getState() + " ,getType: " + event.getType());
			}
		});
		// zk.create("/testCluster", "testCluster".getBytes(),
		// Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.create("/testCluster", "testCluster".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		TimeUnit.SECONDS.sleep(3);
		zk.delete("/testCluster", -1);
		// 关闭连接
		zk.close();
	}

	// ZK的简单操作
	private static void firstZK() throws IOException, KeeperException, InterruptedException {
		// 创建一个与服务器的连接
		ZooKeeper zk = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, TIME_OUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		/**
		 * PERSISTENT:持久化 EPHEMERAL:客户端断开连接时,会删除节点
		 */
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// 创建一个子目录节点
		zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData("/testRootPath", false, null)));
		// 取出子目录节点列表
		System.out.println(zk.getChildren("/testRootPath", true));
		// 修改子目录节点数据
		zk.setData("/testRootPath/testChildPathOne", "modifyChildDataOne".getBytes(), -1);
		System.out.println("目录节点状态：[" + zk.exists("/testRootPath", true) + "]");
		// 创建另外一个子目录节点
		zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo", true, null)));
		// 删除子目录节点
		zk.delete("/testRootPath/testChildPathTwo", -1);
		zk.delete("/testRootPath/testChildPathOne", -1);
		// 删除父目录节点
		zk.delete("/testRootPath", -1);
		// 关闭连接
		zk.close();
	}

}
