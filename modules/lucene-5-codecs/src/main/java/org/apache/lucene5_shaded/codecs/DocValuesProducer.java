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
package org.apache.lucene5_shaded.codecs;


import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Bits;

/** Abstract API that produces numeric, binary, sorted, sortedset,
 *  and sortednumeric docvalues.
 *
 * @lucene.experimental
 */
public abstract class DocValuesProducer implements Closeable, Accountable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesProducer() {}

  /** Returns {@link NumericDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract NumericDocValues getNumeric(FieldInfo field) throws IOException;

  /** Returns {@link BinaryDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract BinaryDocValues getBinary(FieldInfo field) throws IOException;

  /** Returns {@link SortedDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract SortedDocValues getSorted(FieldInfo field) throws IOException;
  
  /** Returns {@link SortedNumericDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException;
  
  /** Returns {@link SortedSetDocValues} for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract SortedSetDocValues getSortedSet(FieldInfo field) throws IOException;
  
  /** Returns a {@link Bits} at the size of <code>reader.maxDoc()</code>, 
   *  with turned on bits for each docid that does have a value for this field.
   *  The returned instance need not be thread-safe: it will only be
   *  used by a single thread. */
  public abstract Bits getDocsWithField(FieldInfo field) throws IOException;
  
  /** 
   * Checks consistency of this producer
   * <p>
   * Note that this may be costly in terms of I/O, e.g. 
   * may involve computing a checksum value against large data files.
   * @lucene.internal
   */
  public abstract void checkIntegrity() throws IOException;
  
  /** 
   * Returns an instance optimized for merging.
   * <p>
   * The default implementation returns {@code this} */
  public DocValuesProducer getMergeInstance() throws IOException {
    return this;
  }
}
