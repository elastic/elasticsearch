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


/**
 * Syntactic sugar for encoding doubles as NumericDocValues
 * via {@link Double#doubleToRawLongBits(double)}.
 * <p>
 * Per-document double values can be retrieved via
 * {@link org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues(String)}.
 * <p>
 * <b>NOTE</b>: In most all cases this will be rather inefficient,
 * requiring eight bytes per document. Consider encoding double
 * values yourself with only as much precision as you require.
 */
public class DoubleDocValuesField extends NumericDocValuesField {

  /** 
   * Creates a new DocValues field with the specified 64-bit double value 
   * @param name field name
   * @param value 64-bit double value
   * @throws IllegalArgumentException if the field name is null
   */
  public DoubleDocValuesField(String name, double value) {
    super(name, Double.doubleToRawLongBits(value));
  }

  @Override
  public void setDoubleValue(double value) {
    super.setLongValue(Double.doubleToRawLongBits(value));
  }
  
  @Override
  public void setLongValue(long value) {
    throw new IllegalArgumentException("cannot change value type from Double to Long");
  }
}
