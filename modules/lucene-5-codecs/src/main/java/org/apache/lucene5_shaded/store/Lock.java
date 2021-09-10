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


import java.io.Closeable;
import java.io.IOException;

/** An interprocess mutex lock.
 * <p>Typical use might look like:<pre class="prettyprint">
 *   try (final Lock lock = directory.obtainLock("my.lock")) {
 *     // ... code to execute while locked ...
 *   }
 * </pre>
 *
 * @see Directory#obtainLock(String)
 *
 * @lucene.internal
 */
public abstract class Lock implements Closeable {

  /** 
   * Releases exclusive access.
   * <p>
   * Note that exceptions thrown from close may require
   * human intervention, as it may mean the lock was no
   * longer valid, or that fs permissions prevent removal
   * of the lock file, or other reasons.
   * <p>
   * {@inheritDoc} 
   * @throws LockReleaseFailedException optional specific exception) if 
   *         the lock could not be properly released.
   */
  public abstract void close() throws IOException;
  
  /** 
   * Best effort check that this lock is still valid. Locks
   * could become invalidated externally for a number of reasons,
   * for example if a user deletes the lock file manually or
   * when a network filesystem is in use. 
   * @throws IOException if the lock is no longer valid.
   */
  public abstract void ensureValid() throws IOException;
}
