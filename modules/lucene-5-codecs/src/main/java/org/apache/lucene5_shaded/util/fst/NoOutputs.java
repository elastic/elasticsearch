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
package org.apache.lucene5_shaded.util.fst;


import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.DataOutput;

/**
 * A null FST {@link Outputs} implementation; use this if
 * you just want to build an FSA.
 *
 * @lucene.experimental
 */

public final class NoOutputs extends Outputs<Object> {

  static final Object NO_OUTPUT = new Object() {
    // NodeHash calls hashCode for this output; we fix this
    // so we get deterministic hashing.
    @Override
    public int hashCode() {
      return 42;
    }

    @Override
    public boolean equals(Object other) {
      return other == this;
    }
  };

  private static final NoOutputs singleton = new NoOutputs();

  private NoOutputs() {
  }

  public static NoOutputs getSingleton() {
    return singleton;
  }

  @Override
  public Object common(Object output1, Object output2) {
    assert output1 == NO_OUTPUT;
    assert output2 == NO_OUTPUT;
    return NO_OUTPUT;
  }

  @Override
  public Object subtract(Object output, Object inc) {
    assert output == NO_OUTPUT;
    assert inc == NO_OUTPUT;
    return NO_OUTPUT;
  }

  @Override
  public Object add(Object prefix, Object output) {
    assert prefix == NO_OUTPUT: "got " + prefix;
    assert output == NO_OUTPUT;
    return NO_OUTPUT;
  }

  @Override
  public Object merge(Object first, Object second) {
    assert first == NO_OUTPUT;
    assert second == NO_OUTPUT;
    return NO_OUTPUT;
  }

  @Override
  public void write(Object prefix, DataOutput out) {
    //assert false;
  }

  @Override
  public Object read(DataInput in) {
    //assert false;
    //return null;
    return NO_OUTPUT;
  }

  @Override
  public Object getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(Object output) {
    return "";
  }

  @Override
  public long ramBytesUsed(Object output) {
    return 0;
  }

  @Override
  public String toString() {
    return "NoOutputs";
  }
}
