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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Holder for suggester and field-level info
 * for a suggest field
 *
 * @lucene.experimental
 */
public final class CompletionsTermsReader implements Accountable {
  /** Minimum entry weight for the suggester */
  public final long minWeight;
  /** Maximum entry weight for the suggester */
  public final long maxWeight;
  /** type of suggester (context-enabled or not) */
  public final byte type;
  private final IndexInput dictIn;
  private final long offset;

  private NRTSuggester suggester;

  /**
   * Creates a CompletionTermsReader to load a field-specific suggester
   * from the index <code>dictIn</code> with <code>offset</code>
   */
  CompletionsTermsReader(IndexInput dictIn, long offset, long minWeight, long maxWeight, byte type) throws IOException {
    assert minWeight <= maxWeight;
    assert offset >= 0l && offset < dictIn.length();
    this.dictIn = dictIn;
    this.offset = offset;
    this.minWeight = minWeight;
    this.maxWeight = maxWeight;
    this.type = type;
  }

  /**
   * Returns the suggester for a field, if not loaded already, loads
   * the appropriate suggester from CompletionDictionary
   */
  public synchronized NRTSuggester suggester() throws IOException {
    if (suggester == null) {
      try (IndexInput dictClone = dictIn.clone()) { // let multiple fields load concurrently
        dictClone.seek(offset);
        suggester = NRTSuggester.load(dictClone);
      }
    }
    return suggester;
  }

  @Override
  public long ramBytesUsed() {
    return (suggester != null) ? suggester.ramBytesUsed() : 0;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
