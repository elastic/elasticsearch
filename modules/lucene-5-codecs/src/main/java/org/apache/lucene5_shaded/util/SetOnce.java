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

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A convenient class which offers a semi-immutable object wrapper
 * implementation which allows one to set the value of an object exactly once,
 * and retrieve it many times. If {@link #set(Object)} is called more than once,
 * {@link AlreadySetException} is thrown and the operation
 * will fail.
 *
 * @lucene.experimental
 */
public final class SetOnce<T> implements Cloneable {

  /** Thrown when {@link SetOnce#set(Object)} is called more than once. */
  public static final class AlreadySetException extends IllegalStateException {
    public AlreadySetException() {
      super("The object cannot be set twice!");
    }
  }
  
  private volatile T obj = null;
  private final AtomicBoolean set;
  
  /**
   * A default constructor which does not set the internal object, and allows
   * setting it by calling {@link #set(Object)}.
   */
  public SetOnce() {
    set = new AtomicBoolean(false);
  }

  /**
   * Creates a new instance with the internal object set to the given object.
   * Note that any calls to {@link #set(Object)} afterwards will result in
   * {@link AlreadySetException}
   *
   * @throws AlreadySetException if called more than once
   * @see #set(Object)
   */
  public SetOnce(T obj) {
    this.obj = obj;
    set = new AtomicBoolean(true);
  }
  
  /** Sets the given object. If the object has already been set, an exception is thrown. */
  public final void set(T obj) {
    if (set.compareAndSet(false, true)) {
      this.obj = obj;
    } else {
      throw new AlreadySetException();
    }
  }
  
  /** Returns the object set by {@link #set(Object)}. */
  public final T get() {
    return obj;
  }
}
