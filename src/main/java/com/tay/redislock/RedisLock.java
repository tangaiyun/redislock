package com.tay.redislock;

import redis.clients.jedis.Jedis;

/**
 * @author Aiyun Tang email: aiyun.tang@gmail.com
 */
public class RedisLock {

	private static final ThreadLocal<Long> expireTimeThreadLocal = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return Long.valueOf(0);
		};
	};
	private Jedis jedis;
	private String lockKey;
	private long timeoutMsecs = 10 * 1000;
	private long expireMsecs = 8 * 1000;
	private volatile boolean destroyed = false;

	public RedisLock(Jedis jedis, String lockKey) {
		this.jedis = jedis;
		this.lockKey = lockKey;
	}

	public RedisLock(Jedis jedis, String lockKey, int timeoutMsecs, int expireMsecs) {
		this(jedis, lockKey);
		this.timeoutMsecs = timeoutMsecs;
		this.expireMsecs = expireMsecs;
	}

	public String getLockKey() {
		return this.lockKey;
	}

	public boolean acquire() throws InterruptedException {
		if(destroyed) {
			throw new RuntimeException("This lock was destroyed, so you can use it again!");
		}
		if (System.currentTimeMillis() < expireTimeThreadLocal.get()) {
			return true;
		}
		long timeout = this.timeoutMsecs;
		while (timeout >= 0) {
			String code = jedis.set(lockKey, "L", "NX", "PX", expireMsecs);
			if ("OK".equals(code)) {
				expireTimeThreadLocal.set(System.currentTimeMillis() + expireMsecs);
				return true;
			}
			timeout -= 10;
			Thread.sleep(10L);
		}

		return false;
	}

	public void release() {
		jedis.del(lockKey);
		expireTimeThreadLocal.remove();
	}
	
	public void destroy() {
		destroyed = true;
		release();
		jedis.close();
	}
}
