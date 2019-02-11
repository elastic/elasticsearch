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

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DoubleValues;

import java.io.IOException;

/**
 * A per-document numeric value.
 */
public abstract class NumericDoubleValues extends DoubleValues {

  /** Sole constructor. (For invocation by subclass
   * constructors, typically implicit.) */
  protected NumericDoubleValues() {}
  
  // TODO: this interaction with sort comparators is really ugly...
  /** Returns numeric docvalues view of raw double bits */
  public NumericDocValues getRawDoubleValues() {
      return new AbstractNumericDocValues() {
          private int docID = -1;
          @Override
          public boolean advanceExact(int target) throws IOException {
              docID = target;
              return NumericDoubleValues.this.advanceExact(target);
          }
          @Override
          public long longValue() throws IOException {
              return Double.doubleToRawLongBits(NumericDoubleValues.this.doubleValue());
          }
          @Override
          public int docID() {
              return docID;
          }
      };
  }
  
  // yes... this is doing what the previous code was doing...
  /** Returns numeric docvalues view of raw float bits */
  public NumericDocValues getRawFloatValues() {
      return new AbstractNumericDocValues() {
          private int docID = -1;
          @Override
          public boolean advanceExact(int target) throws IOException {
              docID = target;
              return NumericDoubleValues.this.advanceExact(target);
          }
          @Override
          public long longValue() throws IOException {
              return Float.floatToRawIntBits((float)NumericDoubleValues.this.doubleValue());
          }
          @Override
          public int docID() {
              return docID;
          }
      };
  }
}
