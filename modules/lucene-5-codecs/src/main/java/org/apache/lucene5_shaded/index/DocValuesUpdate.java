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


import static org.apache.lucene5_shaded.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene5_shaded.util.RamUsageEstimator.NUM_BYTES_CHAR;
import static org.apache.lucene5_shaded.util.RamUsageEstimator.NUM_BYTES_INT;
import static org.apache.lucene5_shaded.util.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
import static org.apache.lucene5_shaded.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import org.apache.lucene5_shaded.document.NumericDocValuesField;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/** An in-place update to a DocValues field. */
abstract class DocValuesUpdate {
  
  /* Rough logic: OBJ_HEADER + 3*PTR + INT
   * Term: OBJ_HEADER + 2*PTR
   *   Term.field: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   *   Term.bytes: 2*OBJ_HEADER + 2*INT + PTR + bytes.length
   * String: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   * T: OBJ_HEADER
   */
  private static final int RAW_SIZE_IN_BYTES = 8*NUM_BYTES_OBJECT_HEADER + 8*NUM_BYTES_OBJECT_REF + 8*NUM_BYTES_INT;
  
  final DocValuesType type;
  final Term term;
  final String field;
  final Object value;
  int docIDUpto = -1; // unassigned until applied, and confusing that it's here, when it's just used in BufferedDeletes...

  /**
   * Constructor.
   * 
   * @param term the {@link Term} which determines the documents that will be updated
   * @param field the {@link NumericDocValuesField} to update
   * @param value the updated value
   */
  protected DocValuesUpdate(DocValuesType type, Term term, String field, Object value) {
    this.type = type;
    this.term = term;
    this.field = field;
    this.value = value;
  }

  abstract long valueSizeInBytes();
  
  final int sizeInBytes() {
    int sizeInBytes = RAW_SIZE_IN_BYTES;
    sizeInBytes += term.field.length() * NUM_BYTES_CHAR;
    sizeInBytes += term.bytes.bytes.length;
    sizeInBytes += field.length() * NUM_BYTES_CHAR;
    sizeInBytes += valueSizeInBytes();
    return sizeInBytes;
  }
  
  @Override
  public String toString() {
    return "term=" + term + ",field=" + field + ",value=" + value + ",docIDUpto=" + docIDUpto;
  }
  
  /** An in-place update to a binary DocValues field */
  static final class BinaryDocValuesUpdate extends DocValuesUpdate {
    
    /* Size of BytesRef: 2*INT + ARRAY_HEADER + PTR */
    private static final long RAW_VALUE_SIZE_IN_BYTES = NUM_BYTES_ARRAY_HEADER + 2*NUM_BYTES_INT + NUM_BYTES_OBJECT_REF;
    
    BinaryDocValuesUpdate(Term term, String field, BytesRef value) {
      super(DocValuesType.BINARY, term, field, value);
    }

    @Override
    long valueSizeInBytes() {
      return RAW_VALUE_SIZE_IN_BYTES + ((BytesRef) value).bytes.length;
    }
    
  }

  /** An in-place update to a numeric DocValues field */
  static final class NumericDocValuesUpdate extends DocValuesUpdate {

    NumericDocValuesUpdate(Term term, String field, Long value) {
      super(DocValuesType.NUMERIC, term, field, value);
    }

    @Override
    long valueSizeInBytes() {
      return RamUsageEstimator.NUM_BYTES_LONG;
    }
    
  }

}
