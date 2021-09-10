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


import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/** Abstraction over an array of longs.
 *  This class extends NumericDocValues so that we don't need to add another
 *  level of abstraction every time we want eg. to use the {@link PackedInts}
 *  utility classes to represent a {@link NumericDocValues} instance.
 *  @lucene.internal */
public abstract class LongValues extends NumericDocValues {

  /** An instance that returns the provided value. */
  public static final LongValues IDENTITY = new LongValues() {

    @Override
    public long get(long index) {
      return index;
    }

  };

  /** Get value at <code>index</code>. */
  public abstract long get(long index);

  @Override
  public long get(int idx) {
    return get((long) idx);
  }

}
