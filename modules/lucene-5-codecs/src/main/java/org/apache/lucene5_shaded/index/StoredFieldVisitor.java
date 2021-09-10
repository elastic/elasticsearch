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


import java.io.IOException;

import org.apache.lucene5_shaded.document.Document;
import org.apache.lucene5_shaded.document.DocumentStoredFieldVisitor;

/**
 * Expert: provides a low-level means of accessing the stored field
 * values in an index.  See {@link IndexReader#document(int,
 * StoredFieldVisitor)}.
 *
 * <p><b>NOTE</b>: a {@code StoredFieldVisitor} implementation
 * should not try to load or visit other stored documents in
 * the same reader because the implementation of stored
 * fields for most codecs is not reeentrant and you will see
 * strange exceptions as a result.
 *
 * <p>See {@link DocumentStoredFieldVisitor}, which is a
 * <code>StoredFieldVisitor</code> that builds the
 * {@link Document} containing all stored fields.  This is
 * used by {@link IndexReader#document(int)}.
 *
 * @lucene.experimental */

public abstract class StoredFieldVisitor {

  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected StoredFieldVisitor() {
  }
  
  /** Process a binary field. 
   * @param value newly allocated byte array with the binary contents. 
   */
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
  }

  /** Process a string field; the provided byte[] value is a UTF-8 encoded string value. */
  public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
  }

  /** Process a int numeric field. */
  public void intField(FieldInfo fieldInfo, int value) throws IOException {
  }

  /** Process a long numeric field. */
  public void longField(FieldInfo fieldInfo, long value) throws IOException {
  }

  /** Process a float numeric field. */
  public void floatField(FieldInfo fieldInfo, float value) throws IOException {
  }

  /** Process a double numeric field. */
  public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
  }
  
  /**
   * Hook before processing a field.
   * Before a field is processed, this method is invoked so that
   * subclasses can return a {@link Status} representing whether
   * they need that particular field or not, or to stop processing
   * entirely.
   */
  public abstract Status needsField(FieldInfo fieldInfo) throws IOException;
  
  /**
   * Enumeration of possible return values for {@link #needsField}.
   */
  public static enum Status {
    /** YES: the field should be visited. */
    YES,
    /** NO: don't visit this field, but continue processing fields for this document. */
    NO,
    /** STOP: don't visit this field and stop processing any other fields for this document. */
    STOP
  }
}
