package com.tay.redislock;

import redis.clients.jedis.Jedis;

/**
 * @author Aiyun Tang
 */
public class RedisLock {

	private Jedis jedis;
	private String lockKey;
	private long timeoutMsecs = 10 * 1000;
	private long expireMsecs = 8 * 1000;
	private boolean locked;

	public RedisLock(Jedis jedis, String lockKey) {
		this.jedis = jedis;
		this.lockKey = lockKey;
		this.locked = false;
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
		long timeout = this.timeoutMsecs;
		while (timeout >= 0) {
			String code = jedis.set(lockKey, "L", "NX", "PX", expireMsecs);
			if ("OK".equals(code)) {
				this.locked = true;
				return true;
			}
			timeout -= 100;
			Thread.sleep(100L);
		}
		return false;
	}

	public synchronized void release() {
		if (locked) {
			jedis.del(lockKey);
			locked = false;
		}
	}

}
