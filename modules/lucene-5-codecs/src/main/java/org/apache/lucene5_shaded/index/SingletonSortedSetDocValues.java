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


import org.apache.lucene5_shaded.util.BytesRef;

/** 
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * that works for single or multi-valued types.
 */
final class SingletonSortedSetDocValues extends RandomAccessOrds {
  private final SortedDocValues in;
  private long currentOrd;
  private long ord;
  
  /** Creates a multi-valued view over the provided SortedDocValues */
  public SingletonSortedSetDocValues(SortedDocValues in) {
    this.in = in;
    assert NO_MORE_ORDS == -1; // this allows our nextOrd() to work for missing values without a check
  }

  /** Return the wrapped {@link SortedDocValues} */
  public SortedDocValues getSortedDocValues() {
    return in;
  }

  @Override
  public long nextOrd() {
    long v = currentOrd;
    currentOrd = NO_MORE_ORDS;
    return v;
  }

  @Override
  public void setDocument(int docID) {
    currentOrd = ord = in.getOrd(docID);
  }

  @Override
  public BytesRef lookupOrd(long ord) {
    // cast is ok: single-valued cannot exceed Integer.MAX_VALUE
    return in.lookupOrd((int) ord);
  }

  @Override
  public long getValueCount() {
    return in.getValueCount();
  }

  @Override
  public long lookupTerm(BytesRef key) {
    return in.lookupTerm(key);
  }

  @Override
  public long ordAt(int index) {
    return ord;
  }

  @Override
  public int cardinality() {
    return (int) (ord >>> 63) ^ 1;
  }

  @Override
  public TermsEnum termsEnum() {
    return in.termsEnum();
  }
}
