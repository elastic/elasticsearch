/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Objects;

/**
 * Encapsulates information about how to perform text searches over a field
 */
public record TextSearchInfo(
    FieldType luceneFieldType,
    SimilarityProvider similarity,
    NamedAnalyzer searchAnalyzer,
    NamedAnalyzer searchQuoteAnalyzer
) {

    private static final FieldType SIMPLE_MATCH_ONLY_FIELD_TYPE = new FieldType();

    static {
        SIMPLE_MATCH_ONLY_FIELD_TYPE.setTokenized(false);
        SIMPLE_MATCH_ONLY_FIELD_TYPE.setOmitNorms(true);
        SIMPLE_MATCH_ONLY_FIELD_TYPE.freeze();
    }

    /**
     * Defines indexing information for fields that support only simple match text queries
     */
    public static final TextSearchInfo SIMPLE_MATCH_ONLY = new TextSearchInfo(
        SIMPLE_MATCH_ONLY_FIELD_TYPE,
        null,
        Lucene.KEYWORD_ANALYZER,
        Lucene.KEYWORD_ANALYZER
    );

    /**
     * Defines indexing information for fields that index as keywords, but split query input
     * on whitespace to build disjunctions.
     */
    public static final TextSearchInfo WHITESPACE_MATCH_ONLY = new TextSearchInfo(
        SIMPLE_MATCH_ONLY_FIELD_TYPE,
        null,
        Lucene.WHITESPACE_ANALYZER,
        Lucene.WHITESPACE_ANALYZER
    );

    /**
     * Defines indexing information for fields that support simple match text queries
     * without using the terms index
     */
    public static final TextSearchInfo SIMPLE_MATCH_WITHOUT_TERMS = new TextSearchInfo(
        SIMPLE_MATCH_ONLY_FIELD_TYPE,
        null,
        Lucene.KEYWORD_ANALYZER,
        Lucene.KEYWORD_ANALYZER
    );

    private static final NamedAnalyzer FORBIDDEN_ANALYZER = new NamedAnalyzer("", AnalyzerScope.GLOBAL, new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            throw new UnsupportedOperationException();
        }
    });

    /**
     * Specifies that this field does not support text searching of any kind
     */
    public static final TextSearchInfo NONE = new TextSearchInfo(
        SIMPLE_MATCH_ONLY_FIELD_TYPE,
        null,
        FORBIDDEN_ANALYZER,
        FORBIDDEN_ANALYZER
    );

    public TextSearchInfo {
        Objects.requireNonNull(searchAnalyzer);
        Objects.requireNonNull(searchQuoteAnalyzer);
    }

    /**
     * @return whether or not this field supports positional queries
     */
    public boolean hasPositions() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    /**
     * @return whether or not this field has indexed offsets for highlighting
     */
    public boolean hasOffsets() {
        return luceneFieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    /**
     * @return whether or not this field has indexed norms
     */
    public boolean hasNorms() {
        return luceneFieldType.omitNorms() == false;
    }

    /**
     * @return whether or not this field is tokenized
     */
    public boolean isTokenized() {
        return luceneFieldType.tokenized();
    }

    /**
     * What sort of term vectors are available
     */
    public enum TermVector {
        NONE,
        DOCS,
        POSITIONS,
        OFFSETS
    }

    /**
     * @return the type of term vectors available for this field
     */
    public TermVector termVectors() {
        if (luceneFieldType.storeTermVectors() == false) {
            return TermVector.NONE;
        }
        if (luceneFieldType.storeTermVectorOffsets()) {
            return TermVector.OFFSETS;
        }
        if (luceneFieldType.storeTermVectorPositions()) {
            return TermVector.POSITIONS;
        }
        return TermVector.DOCS;
    }

}
