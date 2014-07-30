/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Bits.MatchAllBits;

/**
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * that works for single or multi-valued types.
 */
final class SingletonSortedNumericDoubleValues extends SortedNumericDoubleValues {
  private final NumericDoubleValues in;
  private final Bits docsWithField;
  private double value;
  private int count;

  public SingletonSortedNumericDoubleValues(NumericDoubleValues in, Bits docsWithField) {
    this.in = in;
    this.docsWithField = docsWithField instanceof MatchAllBits ? null : docsWithField;
  }

  /** Return the wrapped {@link NumericDoubleValues} */
  public NumericDoubleValues getNumericDoubleValues() {
    return in;
  }

  /** Return the wrapped {@link Bits} */
  public Bits getDocsWithField() {
    return docsWithField;
  }

  @Override
  public void setDocument(int doc) {
    value = in.get(doc);
    if (docsWithField != null && value == 0 && docsWithField.get(doc) == false) {
      count = 0;
    } else {
      count = 1;
    }
  }

  @Override
  public double valueAt(int index) {
    return value;
  }

  @Override
  public int count() {
    return count;
  }
}
