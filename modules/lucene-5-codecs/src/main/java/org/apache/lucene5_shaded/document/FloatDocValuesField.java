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
 * Syntactic sugar for encoding floats as NumericDocValues
 * via {@link Float#floatToRawIntBits(float)}.
 * <p>
 * Per-document floating point values can be retrieved via
 * {@link org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues(String)}.
 * <p>
 * <b>NOTE</b>: In most all cases this will be rather inefficient,
 * requiring four bytes per document. Consider encoding floating
 * point values yourself with only as much precision as you require.
 */
public class FloatDocValuesField extends NumericDocValuesField {

  /** 
   * Creates a new DocValues field with the specified 32-bit float value 
   * @param name field name
   * @param value 32-bit float value
   * @throws IllegalArgumentException if the field name is null
   */
  public FloatDocValuesField(String name, float value) {
    super(name, Float.floatToRawIntBits(value));
  }

  @Override
  public void setFloatValue(float value) {
    super.setLongValue(Float.floatToRawIntBits(value));
  }
  
  @Override
  public void setLongValue(long value) {
    throw new IllegalArgumentException("cannot change value type from Float to Long");
  }
}
