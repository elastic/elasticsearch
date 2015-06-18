package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * <p>
 * Field that indexes a string value and a weight as a weighted completion
 * against a named suggester.
 * Field is tokenized, not stored and stores documents, frequencies and positions.
 * Field can be used to provide near real time document suggestions.
 * </p>
 * <p>
 * Besides the usual {@link org.apache.lucene.analysis.Analyzer}s,
 * {@link org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer}
 * can be used to tune suggest field only parameters
 * (e.g. preserving token seperators, preserving position increments
 * when converting the token stream to an automaton)
 * </p>
 * <p>
 * Example indexing usage:
 * <pre class="prettyprint">
 * document.add(new SuggestField(name, "suggestion", 4));
 * </pre>
 * To perform document suggestions based on the this field, use
 * {@link SuggestIndexSearcher#suggest(CompletionQuery, int)}
 *
 * @lucene.experimental
 */
public class SuggestField extends Field {

  /** Default field type for suggest field */
  public static final FieldType FIELD_TYPE = new FieldType();
  static {
    FIELD_TYPE.setTokenized(true);
    FIELD_TYPE.setStored(false);
    FIELD_TYPE.setStoreTermVectors(false);
    FIELD_TYPE.setOmitNorms(false);
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    FIELD_TYPE.freeze();
  }

  static final byte TYPE = 0;

  private final BytesRef surfaceForm;
  private final int weight;

  /**
   * Creates a {@link SuggestField}
   *
   * @param name   field name
   * @param value  field value to get suggestions on
   * @param weight field weight
   *
   * @throws IllegalArgumentException if either the name or value is null,
   * if value is an empty string, if the weight is negative, if value contains
   * any reserved characters
   */
  public SuggestField(String name, String value, int weight) {
    super(name, value, FIELD_TYPE);
    if (weight < 0) {
      throw new IllegalArgumentException("weight must be >= 0");
    }
    if (value.length() == 0) {
      throw new IllegalArgumentException("value must have a length > 0");
    }
    for (int i = 0; i < value.length(); i++) {
      if (isReserved(value.charAt(i))) {
        throw new IllegalArgumentException("Illegal input [" + value + "] UTF-16 codepoint [0x"
            + Integer.toHexString((int) value.charAt(i))+ "] at position " + i + " is a reserved character");
      }
    }
    this.surfaceForm = new BytesRef(value);
    this.weight = weight;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) throws IOException {
    org.apache.lucene.search.suggest.xdocument.CompletionTokenStream completionStream = wrapTokenStream(super.tokenStream(analyzer, reuse));
    completionStream.setPayload(buildSuggestPayload());
    return completionStream;
  }

  /**
   * Wraps a <code>stream</code> with a CompletionTokenStream.
   *
   * Subclasses can override this method to change the indexing pipeline.
   */
  protected org.apache.lucene.search.suggest.xdocument.CompletionTokenStream wrapTokenStream(TokenStream stream) {
    if (stream instanceof org.apache.lucene.search.suggest.xdocument.CompletionTokenStream) {
      return (org.apache.lucene.search.suggest.xdocument.CompletionTokenStream) stream;
    } else {
      return new org.apache.lucene.search.suggest.xdocument.CompletionTokenStream(stream);
    }
  }

  /**
   * Returns a byte to denote the type of the field
   */
  protected byte type() {
    return TYPE;
  }

  private BytesRef buildSuggestPayload() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream)) {
      output.writeVInt(surfaceForm.length);
      output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
      output.writeVInt(weight + 1);
      output.writeByte(type());
    }
    return new BytesRef(byteArrayOutputStream.toByteArray());
  }

  private boolean isReserved(char c) {
    switch (c) {
      case org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.SEP_LABEL:
      case org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.HOLE_CHARACTER:
      case org.apache.lucene.search.suggest.xdocument.NRTSuggesterBuilder.END_BYTE:
        return true;
      default:
        return false;
    }
  }
}
