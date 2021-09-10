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

/**
 * Use this {@link LockFactory} to disable locking entirely.
 * This is a singleton, you have to use {@link #INSTANCE}.
 *
 * @see LockFactory
 */

public final class NoLockFactory extends LockFactory {

  /** The singleton */
  public static final NoLockFactory INSTANCE = new NoLockFactory();
  
  // visible for AssertingLock!
  static final NoLock SINGLETON_LOCK = new NoLock();
  
  private NoLockFactory() {}

  @Override
  public Lock obtainLock(Directory dir, String lockName) {
    return SINGLETON_LOCK;
  }
  
  private static class NoLock extends Lock {
    @Override
    public void close() {
    }

    @Override
    public void ensureValid() throws IOException {
    }

    @Override
    public String toString() {
      return "NoLock";
    }
  }
}
