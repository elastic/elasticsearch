/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.store;


import java.io.IOException;

import org.apache.lucene5_shaded.util.ThreadInterruptedException;

/** 
 * Directory that wraps another, and that sleeps and retries
 * if obtaining the lock fails.
 * <p>
 * This is not a good idea.
 */
public final class SleepingLockWrapper extends FilterDirectory {
 
  /** 
   * Pass this lockWaitTimeout to try forever to obtain the lock. 
   */
  public static final long LOCK_OBTAIN_WAIT_FOREVER = -1;
  
  /** 
   * How long {@link #obtainLock} waits, in milliseconds,
   * in between attempts to acquire the lock. 
   */
  public static long DEFAULT_POLL_INTERVAL = 1000;
  
  private final long lockWaitTimeout;
  private final long pollInterval;
  
  /**
   * Create a new SleepingLockFactory
   * @param delegate        underlying directory to wrap
   * @param lockWaitTimeout length of time to wait in milliseconds 
   *                        or {@link #LOCK_OBTAIN_WAIT_FOREVER} to retry forever.
   */
  public SleepingLockWrapper(Directory delegate, long lockWaitTimeout) {
    this(delegate, lockWaitTimeout, DEFAULT_POLL_INTERVAL);
  }
  
  /**
   * Create a new SleepingLockFactory
   * @param delegate        underlying directory to wrap
   * @param lockWaitTimeout length of time to wait in milliseconds 
   *                        or {@link #LOCK_OBTAIN_WAIT_FOREVER} to retry forever.
   * @param pollInterval    poll once per this interval in milliseconds until
   *                        {@code lockWaitTimeout} is exceeded.
   */
  public SleepingLockWrapper(Directory delegate, long lockWaitTimeout, long pollInterval) {
    super(delegate);
    this.lockWaitTimeout = lockWaitTimeout;
    this.pollInterval = pollInterval;
    if (lockWaitTimeout < 0 && lockWaitTimeout != LOCK_OBTAIN_WAIT_FOREVER) {
      throw new IllegalArgumentException("lockWaitTimeout should be LOCK_OBTAIN_WAIT_FOREVER or a non-negative number (got " + lockWaitTimeout + ")");
    }
    if (pollInterval < 0) {
      throw new IllegalArgumentException("pollInterval must be a non-negative number (got " + pollInterval + ")");
    }
  }

  @Override
  public Lock obtainLock(String lockName) throws IOException {
    LockObtainFailedException failureReason = null;
    long maxSleepCount = lockWaitTimeout / pollInterval;
    long sleepCount = 0;
    
    do {
      try {
        return in.obtainLock(lockName);
      } catch (LockObtainFailedException failed) {
        if (failureReason == null) {
          failureReason = failed;
        }
      }
      try {
        Thread.sleep(pollInterval);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    } while (sleepCount++ < maxSleepCount || lockWaitTimeout == LOCK_OBTAIN_WAIT_FOREVER);
    
    // we failed to obtain the lock in the required time
    String reason = "Lock obtain timed out: " + this.toString();
    if (failureReason != null) {
      reason += ": " + failureReason;
    }
    throw new LockObtainFailedException(reason, failureReason);
  }

  @Override
  public String toString() {
    return "SleepingLockWrapper(" + in + ")";
  }
}
