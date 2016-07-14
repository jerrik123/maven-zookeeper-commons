package com.mangocity.zoo.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 分布式锁
 */
public class DistributedLock implements Lock, Watcher {
	
	private static final String ZK_CONN = "182.61.33.175:2181";
	
	private ZooKeeper zk = null;
	private String root = "/Lock";// 锁的根目录
	private String lockName;// 竞争资源的标志
	private String waitNode;// 等待前一个锁
	private String myZnode;// 当前锁

	private CountDownLatch latch;// 计数器

	private int sessionTimeout = 10000;

	private boolean isGetLock = false;

	static volatile AtomicInteger count = new AtomicInteger(0);

	private DistributedLock() {
	}

	// 工厂方法
	public static DistributedLock newInstance(String lockName) {
		return new DistributedLock(lockName);
	}

	public static void main(String[] args) throws Exception {
		final long starttime = System.currentTimeMillis();
		final DistributedLock lock = DistributedLock.newInstance("lock_seq_");
		// 模拟多线程并发获得锁
		for (int i = 0; i < 30; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					// DistributedLock lock =
					// DistributedLock.newInstance("lock_seq_");
					while (true) {
						try {
							lock.lock();
							count.incrementAndGet();
							System.out.println(Thread.currentThread().getId() + " getLock() | lock value is: "
									+ count.get());

							TimeUnit.SECONDS.sleep(3);
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							lock.unlock();
							long endtime = System.currentTimeMillis();
							System.err.println(count.get() / ((endtime - starttime) / 1000) + "/s");
						}

					}

				}
			}).start();
		}
	}

	/**
	 * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
	 * 
	 * @param config
	 * @param lockName
	 *            竞争资源标志,lockName中不能包含单词lock
	 */
	private DistributedLock(String lockName) {
		this.lockName = lockName;
		// 创建一个与服务器的连接
		try {
			zk = initZk();
			Stat stat = zk.exists(root, false);
			if (stat == null) {
				// 创建根节点
				zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			throw new LockException(e);
		} catch (InterruptedException e) {
			throw new LockException(e);
		}
	}

	/**
	 * zookeeper节点的监视器
	 */
	public void process(WatchedEvent event) {
		System.out.println("Connection is Building..." + event.getPath() + "-" + event.getState());
		if (this.latch != null) {
			this.latch.countDown();
		}
	}

	public void lock() {
		try {
			if (this.tryLock()) {
				return;
			} else {
				waitForLock(waitNode, sessionTimeout);// 等待锁
			}
		} catch (KeeperException e) {
			throw new LockException(e);
		} catch (InterruptedException e) {
			throw new LockException(e);
		}
	}

	public boolean tryLock() {
		try {
			String splitStr = "_lock_";
			if (lockName.contains(splitStr)) {
				throw new LockException("lockName can not contains \\u000B");
			}
			// 创建临时子节点
			myZnode = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			// 取出所有子节点
			List<String> subNodes = zk.getChildren(root, false);
			// 取出所有lockName的锁
			List<String> lockObjNodes = new ArrayList<String>();
			for (String node : subNodes) {
				String _node = node.split(splitStr)[0];
				if (_node.equals(lockName)) {
					lockObjNodes.add(node);
				}
			}
			Collections.sort(lockObjNodes);// 从小到大排序

			if (myZnode.equals(root + "/" + lockObjNodes.get(0))) {// 如果当前线程创建的节点和锁根目录下的最小节点相同,则获得锁
				System.out.println("threadName: " + Thread.currentThread().getName() + "已经获得锁");
				// 如果是最小的节点,则表示取得锁
				return true;
			}
			// 如果不是最小的节点，找到比自己小1的节点
			String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
			waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);
			System.out.println("没有获得锁,找到比自己小1的节点: " + waitNode);
		} catch (KeeperException e) {
			throw new LockException(e);
		} catch (InterruptedException e) {
			throw new LockException(e);
		}
		return false;
	}

	@SuppressWarnings("finally")
	public boolean tryLock(long time, TimeUnit unit) {
		try {
			if (this.tryLock()) {
				return true;
			}
			return waitForLock(waitNode, time);
		} catch (Exception e) {
			throw new LockException(e);
		} finally {
			return false;
		}
	}

	private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
		Stat stat = zk.exists(root + "/" + lower, true);
		// 判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
		if (stat != null) {
			this.latch = new CountDownLatch(1);
			isGetLock = this.latch.await(waitTime, TimeUnit.MILLISECONDS);
			this.latch = null;
		}
		return true;
	}

	public void unlock() {
		try {
			zk.delete(myZnode, -1);
			myZnode = null;
		} catch (InterruptedException e) {
			throw new LockException(e);
		} catch (KeeperException e) {
			throw new LockException(e);
		}
	}

	/**
	 * 初始化ZK客户端
	 * 
	 * @return
	 */
	public synchronized ZooKeeper initZk() {
		try {
			if (zk == null) {
				zk = new ZooKeeper(ZK_CONN, sessionTimeout, this);
			}
		} catch (IOException e) {
			throw new LockException("zk init connect fail" + e.getMessage());
		}
		return zk;
	}

	public void lockInterruptibly() throws InterruptedException {
		this.lock();
	}

	public Condition newCondition() {
		return null;
	}

	public boolean isGetLock() {
		return isGetLock;
	}

	class LockException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public LockException(String e) {
			super(e);
		}

		public LockException(Exception e) {
			super(e);
		}
	}

}
