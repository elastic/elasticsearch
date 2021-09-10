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
package org.apache.lucene5_shaded.util;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Simple counter class
 * 
 * @lucene.internal
 * @lucene.experimental
 */
public abstract class Counter {

  /**
   * Adds the given delta to the counters current value
   * 
   * @param delta
   *          the delta to add
   * @return the counters updated value
   */
  public abstract long addAndGet(long delta);

  /**
   * Returns the counters current value
   * 
   * @return the counters current value
   */
  public abstract long get();

  /**
   * Returns a new counter. The returned counter is not thread-safe.
   */
  public static Counter newCounter() {
    return newCounter(false);
  }

  /**
   * Returns a new counter.
   * 
   * @param threadSafe
   *          <code>true</code> if the returned counter can be used by multiple
   *          threads concurrently.
   * @return a new counter.
   */
  public static Counter newCounter(boolean threadSafe) {
    return threadSafe ? new AtomicCounter() : new SerialCounter();
  }

  private final static class SerialCounter extends Counter {
    private long count = 0;

    @Override
    public long addAndGet(long delta) {
      return count += delta;
    }

    @Override
    public long get() {
      return count;
    };
  }

  private final static class AtomicCounter extends Counter {
    private final AtomicLong count = new AtomicLong();

    @Override
    public long addAndGet(long delta) {
      return count.addAndGet(delta);
    }

    @Override
    public long get() {
      return count.get();
    }

  }
}
