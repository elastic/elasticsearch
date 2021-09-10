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

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.util.BytesRef;

/** A field whose value is stored so that {@link
 *  IndexSearcher#doc} and {@link IndexReader#document} will
 *  return the field and its value. */
public final class StoredField extends Field {

  /**
   * Type for a stored-only field.
   */
  public final static FieldType TYPE;
  static {
    TYPE = new FieldType();
    TYPE.setStored(true);
    TYPE.freeze();
  }

  /**
   * Create a stored-only field with the given binary value.
   * <p>NOTE: the provided byte[] is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param value byte array pointing to binary content (not copied)
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, byte[] value) {
    super(name, value, TYPE);
  }
  
  /**
   * Create a stored-only field with the given binary value.
   * <p>NOTE: the provided byte[] is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param value byte array pointing to binary content (not copied)
   * @param offset starting position of the byte array
   * @param length valid length of the byte array
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, byte[] value, int offset, int length) {
    super(name, value, offset, length, TYPE);
  }

  /**
   * Create a stored-only field with the given binary value.
   * <p>NOTE: the provided BytesRef is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param value BytesRef pointing to binary content (not copied)
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, BytesRef value) {
    super(name, value, TYPE);
  }

  /**
   * Create a stored-only field with the given string value.
   * @param name field name
   * @param value string value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public StoredField(String name, String value) {
    super(name, value, TYPE);
  }

  // TODO: not great but maybe not a big problem?
  /**
   * Create a stored-only field with the given integer value.
   * @param name field name
   * @param value integer value
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, int value) {
    super(name, TYPE);
    fieldsData = value;
  }

  /**
   * Create a stored-only field with the given float value.
   * @param name field name
   * @param value float value
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, float value) {
    super(name, TYPE);
    fieldsData = value;
  }

  /**
   * Create a stored-only field with the given long value.
   * @param name field name
   * @param value long value
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, long value) {
    super(name, TYPE);
    fieldsData = value;
  }

  /**
   * Create a stored-only field with the given double value.
   * @param name field name
   * @param value double value
   * @throws IllegalArgumentException if the field name is null.
   */
  public StoredField(String name, double value) {
    super(name, TYPE);
    fieldsData = value;
  }
}
