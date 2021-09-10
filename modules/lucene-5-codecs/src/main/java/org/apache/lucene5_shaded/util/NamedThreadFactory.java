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


import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A default {@link ThreadFactory} implementation that accepts the name prefix
 * of the created threads as a constructor argument. Otherwise, this factory
 * yields the same semantics as the thread factory returned by
 * {@link Executors#defaultThreadFactory()}.
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final AtomicInteger threadPoolNumber = new AtomicInteger(1);
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private static final String NAME_PATTERN = "%s-%d-thread";
  private final String threadNamePrefix;

  /**
   * Creates a new {@link NamedThreadFactory} instance
   * 
   * @param threadNamePrefix the name prefix assigned to each thread created.
   */
  public NamedThreadFactory(String threadNamePrefix) {
    final SecurityManager s = System.getSecurityManager();
    group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
        .getThreadGroup();
    this.threadNamePrefix = String.format(Locale.ROOT, NAME_PATTERN,
        checkPrefix(threadNamePrefix), threadPoolNumber.getAndIncrement());
  }

  private static String checkPrefix(String prefix) {
    return prefix == null || prefix.length() == 0 ? "Lucene" : prefix;
  }

  /**
   * Creates a new {@link Thread}
   * 
   * @see ThreadFactory#newThread(Runnable)
   */
  @Override
  public Thread newThread(Runnable r) {
    final Thread t = new Thread(group, r, String.format(Locale.ROOT, "%s-%d",
        this.threadNamePrefix, threadNumber.getAndIncrement()), 0);
    t.setDaemon(false);
    t.setPriority(Thread.NORM_PRIORITY);
    return t;
  }

}
