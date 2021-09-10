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
package org.apache.lucene5_shaded.document;


import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.util.NumericUtils;

/**
 * <p>
 * Field that stores a per-document <code>long</code> values for scoring, 
 * sorting or value retrieval. Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new SortedNumericDocValuesField(name, 5L));
 *   document.add(new SortedNumericDocValuesField(name, 14L));
 * </pre>
 * 
 * <p>
 * Note that if you want to encode doubles or floats with proper sort order,
 * you will need to encode them with {@link NumericUtils}:
 * 
 * <pre class="prettyprint">
 *   document.add(new SortedNumericDocValuesField(name, NumericUtils.floatToSortableInt(-5.3f)));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * */

public class SortedNumericDocValuesField extends Field {

  /**
   * Type for sorted numeric DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }

  /** 
   * Creates a new DocValues field with the specified 64-bit long value 
   * @param name field name
   * @param value 64-bit long value
   * @throws IllegalArgumentException if the field name is null
   */
  public SortedNumericDocValuesField(String name, long value) {
    super(name, TYPE);
    fieldsData = Long.valueOf(value);
  }
}
