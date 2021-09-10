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
package org.apache.lucene5_shaded.index;


/** 
 * Extension of {@link SortedSetDocValues} that supports random access
 * to the ordinals of a document.
 * <p>
 * Operations via this API are independent of the iterator api ({@link #nextOrd()})
 * and do not impact its state.
 * <p>
 * Codecs can optionally extend this API if they support constant-time access
 * to ordinals for the document.
 */
public abstract class RandomAccessOrds extends SortedSetDocValues {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected RandomAccessOrds() {}

  /** 
   * Retrieve the ordinal for the current document (previously
   * set by {@link #setDocument(int)} at the specified index.
   * <p>
   * An index ranges from {@code 0} to {@code cardinality()-1}.
   * The first ordinal value is at index {@code 0}, the next at index {@code 1},
   * and so on, as for array indexing.
   * @param index index of the ordinal for the document.
   * @return ordinal for the document at the specified index.
   */
  public abstract long ordAt(int index);
  
  /** 
   * Returns the cardinality for the current document (previously
   * set by {@link #setDocument(int)}.
   */
  public abstract int cardinality();
}
