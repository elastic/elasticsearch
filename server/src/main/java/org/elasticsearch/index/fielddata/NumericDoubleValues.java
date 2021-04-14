/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
