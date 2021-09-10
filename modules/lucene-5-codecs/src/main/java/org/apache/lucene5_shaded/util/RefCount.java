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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Manages reference counting for a given object. Extensions can override
 * {@link #release()} to do custom logic when reference counting hits 0.
 */
public class RefCount<T> {
  
  private final AtomicInteger refCount = new AtomicInteger(1);
  
  protected final T object;
  
  public RefCount(T object) {
    this.object = object;
  }

  /**
   * Called when reference counting hits 0. By default this method does nothing,
   * but extensions can override to e.g. release resources attached to object
   * that is managed by this class.
   */
  protected void release() throws IOException {}
  
  /**
   * Decrements the reference counting of this object. When reference counting
   * hits 0, calls {@link #release()}.
   */
  public final void decRef() throws IOException {
    final int rc = refCount.decrementAndGet();
    if (rc == 0) {
      boolean success = false;
      try {
        release();
        success = true;
      } finally {
        if (!success) {
          // Put reference back on failure
          refCount.incrementAndGet();
        }
      }
    } else if (rc < 0) {
      throw new IllegalStateException("too many decRef calls: refCount is " + rc + " after decrement");
    }
  }
  
  public final T get() {
    return object;
  }
  
  /** Returns the current reference count. */
  public final int getRefCount() {
    return refCount.get();
  }
  
  /**
   * Increments the reference count. Calls to this method must be matched with
   * calls to {@link #decRef()}.
   */
  public final void incRef() {
    refCount.incrementAndGet();
  }
  
}

