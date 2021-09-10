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


import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;

/**
  A Term represents a word from text.  This is the unit of search.  It is
  composed of two elements, the text of the word, as a string, and the name of
  the field that the text occurred in.

  Note that terms may represent more than words from text fields, but also
  things like dates, email addresses, urls, etc.  */

public final class Term implements Comparable<Term> {
  String field;
  BytesRef bytes;

  /** Constructs a Term with the given field and bytes.
   * <p>Note that a null field or null bytes value results in undefined
   * behavior for most Lucene APIs that accept a Term parameter.
   *
   * <p>The provided BytesRef is copied when it is non null.
   */
  public Term(String fld, BytesRef bytes) {
    field = fld;
    this.bytes = bytes == null ? null : BytesRef.deepCopyOf(bytes);
  }

  /** Constructs a Term with the given field and the bytes from a builder.
   * <p>Note that a null field value results in undefined
   * behavior for most Lucene APIs that accept a Term parameter.
   */
  public Term(String fld, BytesRefBuilder bytesBuilder) {
    field = fld;
    this.bytes = bytesBuilder.toBytesRef();
  }

  /** Constructs a Term with the given field and text.
   * <p>Note that a null field or null text value results in undefined
   * behavior for most Lucene APIs that accept a Term parameter. */
  public Term(String fld, String text) {
    this(fld, new BytesRef(text));
  }

  /** Constructs a Term with the given field and empty text.
   * This serves two purposes: 1) reuse of a Term with the same field.
   * 2) pattern for a query.
   *
   * @param fld field's name
   */
  public Term(String fld) {
    this(fld, new BytesRef());
  }

  /** Returns the field of this term.   The field indicates
    the part of a document which this term came from. */
  public final String field() { return field; }

  /** Returns the text of this term.  In the case of words, this is simply the
    text of the word.  In the case of dates and other types, this is an
    encoding of the object as a string.  */
  public final String text() {
    return toString(bytes);
  }

  /** Returns human-readable form of the term text. If the term is not unicode,
   * the raw bytes will be printed instead. */
  public static final String toString(BytesRef termText) {
    // the term might not be text, but usually is. so we make a best effort
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      return decoder.decode(ByteBuffer.wrap(termText.bytes, termText.offset, termText.length)).toString();
    } catch (CharacterCodingException e) {
      return termText.toString();
    }
  }

  /** Returns the bytes of this term, these should not be modified. */
  public final BytesRef bytes() { return bytes; }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Term other = (Term) obj;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
      return false;
    if (bytes == null) {
      if (other.bytes != null)
        return false;
    } else if (!bytes.equals(other.bytes))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((bytes == null) ? 0 : bytes.hashCode());
    return result;
  }

  /** Compares two terms, returning a negative integer if this
    term belongs before the argument, zero if this term is equal to the
    argument, and a positive integer if this term belongs after the argument.

    The ordering of terms is first by field, then by text.*/
  @Override
  public final int compareTo(Term other) {
    if (field.equals(other.field)) {
      return bytes.compareTo(other.bytes);
    } else {
      return field.compareTo(other.field);
    }
  }

  /**
   * Resets the field and text of a Term.
   * <p>WARNING: the provided BytesRef is not copied, but used directly.
   * Therefore the bytes should not be modified after construction, for
   * example, you should clone a copy rather than pass reused bytes from
   * a TermsEnum.
   */
  final void set(String fld, BytesRef bytes) {
    field = fld;
    this.bytes = bytes;
  }

  @Override
  public final String toString() { return field + ":" + text(); }
}
