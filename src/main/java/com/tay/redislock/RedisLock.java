package com.tay.redislock;

import java.util.concurrent.atomic.AtomicBoolean;

import redis.clients.jedis.Jedis;

/**
 * @author Aiyun Tang
 * email: aiyun.tang@gmail.com
 */
public class RedisLock {

	private AtomicBoolean isacquired = new AtomicBoolean();;
	private AtomicBoolean isReleased = new AtomicBoolean();
	private AtomicBoolean isLocked = new AtomicBoolean();	
	private static ThreadLocal<Boolean> lockAcquiredThreadLocal = new ThreadLocal<Boolean>(); 
	private Jedis jedis;
	private String lockKey;
	private long timeoutMsecs = 10 * 1000;
	private long expireMsecs = 8 * 1000;
	
	public RedisLock(Jedis jedis, String lockKey) {
		this.jedis = jedis;
		this.lockKey = lockKey;
		this.isLocked.set(false);
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
		if(isReleased.get()) {
			throw new RuntimeException("This lock instance with lockkey: " + lockKey + " was released, it can not been used again.");
		}
		if(isacquired.get()) {
			throw new RuntimeException("This lock instance with lockkey: " + lockKey+ " was acquired, it can not been used again.");
		}
		if(isacquired.getAndSet(true) == false)
		{
			long timeout = this.timeoutMsecs;
			while (timeout >= 0) {
				String code = jedis.set(lockKey, "L", "NX", "PX", expireMsecs);
				if ("OK".equals(code)) {
					isLocked.set(true);
					lockAcquiredThreadLocal.set(true);
					return true;
				}
				timeout -= 10;
				Thread.sleep(10L);
			}
		}
		return false;
	}

	public void release() {
		if(isLocked.get() && !lockAcquiredThreadLocal.get()) {
			throw new RuntimeException("This lock instance with lockkey: " + lockKey + " should been released by the lock acquired thread.");
		}
		if (isLocked.get() && lockAcquiredThreadLocal.get()) {
			isReleased.set(true); 
			jedis.del(lockKey);
			isLocked.set(false);
			lockAcquiredThreadLocal.remove();
			jedis.close();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		lockAcquiredThreadLocal.remove();
		super.finalize();
	}
}
