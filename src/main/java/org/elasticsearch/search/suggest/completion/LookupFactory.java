/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;

abstract class LookupFactory {
    public abstract Lookup getLookup(CompletionFieldMapper mapper, CompletionSuggestionContext suggestionContext);

    public abstract CompletionStats stats(String... fields);

    abstract AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder getAnalyzingSuggestHolder(CompletionFieldMapper mapper);

    public abstract long ramBytesUsed();

    public static final class CompletionTerms extends FilterAtomicReader.FilterTerms {
        private final LookupFactory lookup;

        public CompletionTerms(Terms delegate, LookupFactory lookup) {
            super(delegate);
            this.lookup = lookup;
        }

        public Lookup getLookup(CompletionFieldMapper mapper, CompletionSuggestionContext suggestionContext) {
            return lookup.getLookup(mapper, suggestionContext);
        }

        public CompletionStats stats(String ... fields) {
            return lookup.stats(fields);
        }
    }
}

