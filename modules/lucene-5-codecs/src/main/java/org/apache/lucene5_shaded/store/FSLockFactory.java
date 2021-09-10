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
 * Base class for file system based locking implementation.
 * This class is explicitly checking that the passed {@link Directory}
 * is an {@link FSDirectory}.
 */
public abstract class FSLockFactory extends LockFactory {
  
  /** Returns the default locking implementation for this platform.
   * This method currently returns always {@link NativeFSLockFactory}.
   */
  public static final FSLockFactory getDefault() {
    return NativeFSLockFactory.INSTANCE;
  }

  @Override
  public final Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof FSDirectory)) {
      throw new UnsupportedOperationException(getClass().getSimpleName() + " can only be used with FSDirectory subclasses, got: " + dir);
    }
    return obtainFSLock((FSDirectory) dir, lockName);
  }
  
  /** 
   * Implement this method to obtain a lock for a FSDirectory instance. 
   * @throws IOException if the lock could not be obtained.
   */
  protected abstract Lock obtainFSLock(FSDirectory dir, String lockName) throws IOException;

}
