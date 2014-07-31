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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.suggest.analyzing.LookupPostingsFormat.LookupBuildConfiguration;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;
import java.util.Set;

/**
 * Class responsible for initialing and generating lookup fields
 * Usage:
 * LookupFieldGenerator generator = LookupFieldGenerator.create(name, ..)
 * generator.generate(value, weight)
 *
 */
public class LookupFieldGenerator {

    private final String name;
    private final LookupTokenStream.ToFiniteStrings toFiniteStrings;
    private final PayloadProcessor payloadProcessor;
    private final Analyzer indexAnalyzer;
    private final String type;
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        FIELD_TYPE.freeze();
    }

    /**
     * Creates a lookup field generator
     * @param name of the lookup field
     * @param indexAnalyzer used to analyze tokens at index time
     * @param queryAnalyzer used to analyze the query prefix
     * @param scorer to be used to score the results
     * @param preserveSep configuration to preserve seperators at lookup and index time
     * @param preservePositionIncrements configuration to preserve position holes at lookup and index time
     */
    private LookupFieldGenerator(String name, Analyzer indexAnalyzer, Analyzer queryAnalyzer, final ContextAwareScorer scorer,
                                 final boolean preserveSep, final boolean preservePositionIncrements) {
        this.name = name;
        this.indexAnalyzer = indexAnalyzer;
        this.toFiniteStrings = new LookupTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                return NRTSuggester.TokenUtils.toFiniteStrings(stream, preserveSep, preservePositionIncrements);
            }
        };

        if (scorer instanceof ContextAwareScorer.LongBased) {
            payloadProcessor = new PayloadProcessor.LongPayloadProcessor();
            this.type = Long.class.getName();
        } else if (scorer instanceof ContextAwareScorer.BytesBased) {
            payloadProcessor = new PayloadProcessor.BytesRefPayloadProcessor();
            this.type = BytesRef.class.getName();
        } else {
            throw new IllegalArgumentException("scorer has to be type ContextAwareScorer.LongBased or ContextAwareScorer.BytesBased");
        }

        LookupPostingsFormat.register(name,
                new LookupBuildConfiguration(scorer, queryAnalyzer, preserveSep, preservePositionIncrements, payloadProcessor));
    }

    /**
     * @return index analyzer for the lookup field
     * TODO: this indexAnalyzer should be enforced
     */
    public Analyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    /**
     * Generates a lookup field
     * @param value of the field
     * @param weight associated with the field
     * @return a lookup field
     */
    public Field generate(String value, Long weight) {
        if (!type.equals(Long.class.getName())) {
            throw new IllegalArgumentException("weight can not be of type Long");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can not be null");
        }
        if (weight == null) {
            throw new IllegalArgumentException("weight value can not be null");
        }
        return new LookupField<Long>(name, value, FIELD_TYPE, weight, toFiniteStrings, payloadProcessor);
    }

    /**
     * Generates a lookup field
     * @param value of the field
     * @param weight associated with the field
     * @return a lookup field
     */
    public Field generate(String value, BytesRef weight) {
        if (!type.equals(BytesRef.class.getName())) {
            throw new IllegalArgumentException("weight can not be of type BytesRef");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can not be null");
        }
        if (weight == null) {
            throw new IllegalArgumentException("weight value can not be null");
        }
        return new LookupField<BytesRef>(name, value, FIELD_TYPE, weight, toFiniteStrings, payloadProcessor);
    }

    /**
     * Creates a {@link LookupFieldGenerator} with a default long scorer
     *
     * @param name unique lookup name
     * @param indexAnalyzer used to analyze tokens at index time
     * @param queryAnalyzer used to analyze the query prefix
     * @param preserveSep configuration to preserve seperators at lookup and index time
     * @param preservePositionIncrements configuration to preserve position holes at lookup and index time
     */
    public static LookupFieldGenerator create(String name, Analyzer indexAnalyzer, Analyzer queryAnalyzer, boolean preserveSep, boolean preservePositionIncrements) {
        return LookupFieldGenerator.create(name, indexAnalyzer, queryAnalyzer, preserveSep, preservePositionIncrements, ContextAwareScorer.DEFAULT);
    }

    /**
     * Creates a {@link LookupFieldGenerator} preserving seperators and position increments at index and query time
     * with a default long scorer
     *
     * @param name unique lookup name
     * @param analyzer used at index and query time
     */
    public static LookupFieldGenerator create(String name, Analyzer analyzer) {
        return LookupFieldGenerator.create(name, analyzer, analyzer, true, true);
    }

    /**
     * Creates a {@link LookupFieldGenerator} with a default long scorer
     *
     * @param name unique lookup name
     * @param analyzer analyzer used at index and query time
     * @param preserveSep configuration to preserve seperators at lookup and index time
     * @param preservePositionIncrements configuration to preserve position holes at lookup and index time
     */
    public static LookupFieldGenerator create(String name, Analyzer analyzer, boolean preserveSep, boolean preservePositionIncrements) {
        return LookupFieldGenerator.create(name, analyzer, analyzer, preserveSep, preservePositionIncrements);
    }

    /**
     * Creates a {@link LookupFieldGenerator} preserving seperators and position increments at index and query time
     * with a default long scorer
     *
     * @param name unique lookup name
     * @param indexAnalyzer used to analyze tokens at index time
     * @param queryAnalyzer used to analyze the query prefix
     */
    public static LookupFieldGenerator create(String name, Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
        return LookupFieldGenerator.create(name, indexAnalyzer, queryAnalyzer, true, true);
    }

    /**
     * Creates a {@link LookupFieldGenerator} instance
     *
     * @param name unique lookup name
     * @param indexAnalyzer used to analyze tokens at index time
     * @param queryAnalyzer used to analyze the query prefix
     * @param preserveSep configuration to preserve seperators at lookup and index time
     * @param preservePositionIncrements configuration to preserve position holes at lookup and index time
     * @param scorer to configure score output and scoring
     */
    public static LookupFieldGenerator create(String name, Analyzer indexAnalyzer, Analyzer queryAnalyzer,
                                              final boolean preserveSep, final boolean preservePositionIncrements,
                                              final ContextAwareScorer scorer) {
        return new LookupFieldGenerator(name, indexAnalyzer, queryAnalyzer, scorer, preserveSep, preservePositionIncrements);
    }

    private static final class LookupField<T> extends Field {
        public static final char UNIT_SEPARATOR = '\u001f';

        private final BytesRef surfaceForm;
        private final T weight;

        private final LookupTokenStream.ToFiniteStrings toFiniteStrings;
        private final PayloadProcessor<T> payloadProcessor;

        LookupField(String name, String value, FieldType type, T weight, LookupTokenStream.ToFiniteStrings toFiniteStrings, PayloadProcessor<T> payloadProcessor) {
            super(name, value, type);
            this.surfaceForm = new BytesRef(value);
            this.weight = weight;
            this.payloadProcessor = payloadProcessor;
            this.toFiniteStrings = toFiniteStrings;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) throws IOException {
            BytesRef suggestPayload = buildSuggestPayload(surfaceForm, weight);
            return new LookupTokenStream(super.tokenStream(analyzer, previous), suggestPayload, toFiniteStrings);
        }

        private BytesRef buildSuggestPayload(BytesRef surfaceForm, T weight) throws IOException {
            for (int i = 0; i < surfaceForm.length; i++) {
                if (surfaceForm.bytes[i] == UNIT_SEPARATOR) {
                    throw new IllegalArgumentException(
                            "surface form cannot contain unit separator character U+001F; this character is reserved");
                }
            }
            return payloadProcessor.build(surfaceForm, weight);
        }
    }
}
