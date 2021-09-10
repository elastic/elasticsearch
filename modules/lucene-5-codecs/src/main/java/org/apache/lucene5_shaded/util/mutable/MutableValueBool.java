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
 * {@link MutableValue} implementation of type <code>boolean</code>.
 * When mutating instances of this object, the caller is responsible for ensuring 
 * that any instance where <code>exists</code> is set to <code>false</code> must also 
 * <code>value</code> set to <code>false</code> for proper operation.
 */
public class MutableValueBool extends MutableValue {
  public boolean value;

  @Override
  public Object toObject() {
    assert exists || (false == value);
    return exists ? value : null;
  }

  @Override
  public void copy(MutableValue source) {
    MutableValueBool s = (MutableValueBool) source;
    value = s.value;
    exists = s.exists;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueBool v = new MutableValueBool();
    v.value = this.value;
    v.exists = this.exists;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    assert exists || (false == value);
    MutableValueBool b = (MutableValueBool)other;
    return value == b.value && exists == b.exists;
  }

  @Override
  public int compareSameType(Object other) {
    assert exists || (false == value);
    MutableValueBool b = (MutableValueBool)other;
    if (value != b.value) return value ? 1 : -1;
    if (exists == b.exists) return 0;
    return exists ? 1 : -1;
  }

  @Override
  public int hashCode() {
    assert exists || (false == value);
    return value ? 2 : (exists ? 1 : 0);
  }
}
