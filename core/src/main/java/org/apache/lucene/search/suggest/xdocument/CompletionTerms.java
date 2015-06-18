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

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Wrapped {@link org.apache.lucene.index.Terms}
 * used by {@link SuggestField} and {@link ContextSuggestField}
 * to access corresponding suggester and their attributes
 *
 * @lucene.experimental
 */
public final class CompletionTerms extends FilterLeafReader.FilterTerms implements Accountable {

  private final CompletionsTermsReader reader;

  /**
   * Creates a completionTerms based on {@link CompletionsTermsReader}
   */
  CompletionTerms(Terms in, CompletionsTermsReader reader) {
    super(in);
    this.reader = reader;
  }

  /**
   * Returns the type of FST, either {@link SuggestField#TYPE} or
   * {@link ContextSuggestField#TYPE}
   */
  public byte getType() {
    return (reader != null) ? reader.type : SuggestField.TYPE;
  }

  /**
   * Returns the minimum weight of all entries in the weighted FST
   */
  public long getMinWeight() {
    return (reader != null) ? reader.minWeight : 0;
  }

  /**
   * Returns the maximum weight of all entries in the weighted FST
   */
  public long getMaxWeight() {
    return (reader != null) ? reader.maxWeight : 0;
  }

  /**
   * Returns a {@link NRTSuggester} for the field
   * or <code>null</code> if no FST
   * was indexed for this field
   */
  public NRTSuggester suggester() throws IOException {
    return (reader != null) ? reader.suggester() : null;
  }

  @Override
  public long ramBytesUsed() {
    return (reader != null) ? reader.ramBytesUsed() : 0;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return (reader != null) ? reader.getChildResources() : Collections.<Accountable>emptyList();
  }
}
