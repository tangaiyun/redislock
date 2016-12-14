package com.tay.redislock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisLockTest {
	Jedis jedis = new Jedis("192.168.30.118");
	@Test
	public void testLock() throws InterruptedException {
		String testLockKey = "testLockKey1";
		RedisLock rlock = new RedisLock(jedis, testLockKey, 10000, 6000);
		Assert.assertTrue(rlock.acquire());
		rlock.release();

	}
	
	@Test
	public void testLockAutoDelete() throws InterruptedException {
		String testLockKey = "testLockAutoDelete";
		RedisLock rlock = new RedisLock(jedis, testLockKey, 10000, 6000);
		Assert.assertTrue(rlock.acquire());
		System.out.println(jedis.ttl(testLockKey));
		Thread.sleep(6100);
		Assert.assertNull(jedis.get(testLockKey));
	}

	@Test
	public void testLock2() throws InterruptedException, ExecutionException {
		String testLockKey = "testLockKey2";
		RedisLock rlock = new RedisLock(jedis, testLockKey, 20000, 30000);
		ExecutorService executor = Executors.newFixedThreadPool(5);
		CompletableFuture<Boolean> f1 = CompletableFuture.supplyAsync(() -> {
			try {
				return rlock.acquire();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}, executor);
		CompletableFuture<Boolean> f2 = f1.thenApply(new Function<Boolean, Boolean>() {

			@Override
			public Boolean apply(Boolean b) {
				try {
					return rlock.acquire();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});
		Assert.assertFalse(f2.get());
		System.out.println(jedis.pttl(testLockKey));
	}

	@Test
	public void testLock3() throws InterruptedException, ExecutionException {
		String testLockKey = "testLockKey3";
		RedisLock rlock = new RedisLock(jedis, testLockKey, 6000, 4000);
		ExecutorService executor = Executors.newFixedThreadPool(5);
		CompletableFuture<Boolean> f1 = CompletableFuture.supplyAsync(() -> {
			try {
				return rlock.acquire();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}, executor);
		CompletableFuture<Boolean> f2 = f1.thenApplyAsync((new Function<Boolean, Boolean>() {

			@Override
			public Boolean apply(Boolean b) {
				try {
					System.out.println("Thread1 lock acquire:" + b);
					long t1 = System.currentTimeMillis();
					boolean bool = rlock.acquire();
					System.out.println("Thread2 acquire:" + b);
					long t2 = System.currentTimeMillis();
					System.out.println(t2 - t1);
					return bool;

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		}), executor);
		System.out.println(f2.get());
		Assert.assertTrue(f2.get());
		System.out.println(jedis.pttl(testLockKey));
	}

	
	@Test
	public void testLock6() throws InterruptedException, ExecutionException {
		String testLockKey = "testLockKey6";
		RedisLock rlock = new RedisLock(jedis, testLockKey, 5000, 3000);
		ExecutorService executor = Executors.newFixedThreadPool(5);
		CompletableFuture<Boolean> f1 = CompletableFuture.supplyAsync(() -> {
			try {

				boolean b = rlock.acquire();
				System.out.println(jedis.pttl(testLockKey));
				return b;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}, executor);
		CompletableFuture<Boolean> f2 = f1.thenApplyAsync((new Function<Boolean, Boolean>() {

			@Override
			public Boolean apply(Boolean b) {
				try {
					System.out.println("Thread1 lock acquire:" + b);
					long t1 = System.currentTimeMillis();
					boolean bool = rlock.acquire();
					System.out.println("Thread2 acquire:" + b);
					long t2 = System.currentTimeMillis();
					System.out.println(t2 - t1);
					return bool;

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		}), executor);
		System.out.println(f2.get());
		Assert.assertTrue(f2.get());
		Thread.sleep(10000);
		System.out.println(jedis.get(testLockKey));
	}
}
