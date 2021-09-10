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
 * Base implementation for a concrete {@link Directory} that uses a {@link LockFactory} for locking.
 * @lucene.experimental
 */
public abstract class BaseDirectory extends Directory {

  volatile protected boolean isOpen = true;

  /** Holds the LockFactory instance (implements locking for
   * this Directory instance). */
  protected final LockFactory lockFactory;

  /** Sole constructor. */
  protected BaseDirectory(LockFactory lockFactory) {
    super();
    if (lockFactory == null) {
      throw new NullPointerException("LockFactory cannot be null, use an explicit instance!");
    }
    this.lockFactory = lockFactory;
  }

  @Override
  public final Lock obtainLock(String name) throws IOException {
    return lockFactory.obtainLock(this, name);
  }

  @Override
  protected final void ensureOpen() throws AlreadyClosedException {
    if (!isOpen)
      throw new AlreadyClosedException("this Directory is closed");
  }

  @Override
  public String toString() {
    return super.toString()  + " lockFactory=" + lockFactory;
  }
  
}
