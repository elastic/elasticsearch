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
package org.apache.lucene5_shaded.util.mutable;

/**
 * Base class for all mutable values.
 *  
 * @lucene.internal 
 */
public abstract class MutableValue implements Comparable<MutableValue> {
  public boolean exists = true;

  public abstract void copy(MutableValue source);
  public abstract MutableValue duplicate();
  public abstract boolean equalsSameType(Object other);
  public abstract int compareSameType(Object other);
  public abstract Object toObject();

  public boolean exists() {
    return exists;
  }

  @Override
  public int compareTo(MutableValue other) {
    Class<? extends MutableValue> c1 = this.getClass();
    Class<? extends MutableValue> c2 = other.getClass();
    if (c1 != c2) {
      int c = c1.hashCode() - c2.hashCode();
      if (c == 0) {
        c = c1.getCanonicalName().compareTo(c2.getCanonicalName());
      }
      return c;
    }
    return compareSameType(other);
  }

  @Override
  public boolean equals(Object other) {
    return (getClass() == other.getClass()) && this.equalsSameType(other);
  }

  @Override
  public abstract int hashCode();

  @Override
  public String toString() {
    return exists() ? toObject().toString() : "(null)";
  }
}


