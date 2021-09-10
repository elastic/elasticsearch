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
import java.util.HashSet;

/**
 * Implements {@link LockFactory} for a single in-process instance,
 * meaning all locking will take place through this one instance.
 * Only use this {@link LockFactory} when you are certain all
 * IndexWriters for a given index are running
 * against a single shared in-process Directory instance.  This is
 * currently the default locking for RAMDirectory.
 *
 * @see LockFactory
 */

public final class SingleInstanceLockFactory extends LockFactory {

  final HashSet<String> locks = new HashSet<>();

  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    synchronized (locks) {
      if (locks.add(lockName)) {
        return new SingleInstanceLock(lockName);
      } else {
        throw new LockObtainFailedException("lock instance already obtained: (dir=" + dir + ", lockName=" + lockName + ")");
      }
    }
  }

  private class SingleInstanceLock extends Lock {
    private final String lockName;
    private volatile boolean closed;

    public SingleInstanceLock(String lockName) {
      this.lockName = lockName;
    }

    @Override
    public void ensureValid() throws IOException {
      if (closed) {
        throw new AlreadyClosedException("Lock instance already released: " + this);
      }
      // check we are still in the locks map (some debugger or something crazy didn't remove us)
      synchronized (locks) {
        if (!locks.contains(lockName)) {
          throw new AlreadyClosedException("Lock instance was invalidated from map: " + this);
        }
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      try {
        synchronized (locks) {
          if (!locks.remove(lockName)) {
            throw new AlreadyClosedException("Lock was already released: " + this);
          }
        }
      } finally {
        closed = true;
      }
    }

    @Override
    public String toString() {
      return super.toString() + ": " + lockName;
    }
  }
}
