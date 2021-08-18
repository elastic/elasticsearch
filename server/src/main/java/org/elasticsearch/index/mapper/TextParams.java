/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper.Parameter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility functions for text mapper parameters
 */
public final class TextParams {

    private TextParams() {}

    public static final class Analyzers {
        public final Parameter<NamedAnalyzer> indexAnalyzer;
        public final Parameter<NamedAnalyzer> searchAnalyzer;
        public final Parameter<NamedAnalyzer> searchQuoteAnalyzer;
        public final Parameter<Integer> positionIncrementGap;
        public final IndexAnalyzers indexAnalyzers;

        public Analyzers(IndexAnalyzers indexAnalyzers,
                         Function<FieldMapper, Analyzers> analyzerInitFunction) {
            this.indexAnalyzer = Parameter.analyzerParam("analyzer", false,
                m -> analyzerInitFunction.apply(m).indexAnalyzer.get(), indexAnalyzers::getDefaultIndexAnalyzer)
                .setSerializerCheck((id, ic, a) -> id || ic ||
                    Objects.equals(a, getSearchAnalyzer()) == false || Objects.equals(a, getSearchQuoteAnalyzer()) == false)
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.INDEX_TIME));
            this.searchAnalyzer
                = Parameter.analyzerParam("search_analyzer", true,
                m -> m.fieldType().getTextSearchInfo().getSearchAnalyzer(), () -> {
                    if (indexAnalyzer.isConfigured() == false) {
                        NamedAnalyzer defaultAnalyzer = indexAnalyzers.get(AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME);
                        if (defaultAnalyzer != null) {
                            return defaultAnalyzer;
                        }
                    }
                    return indexAnalyzer.get();
                })
                .setSerializerCheck((id, ic, a) -> id || ic || Objects.equals(a, getSearchQuoteAnalyzer()) == false)
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.SEARCH_TIME));
            this.searchQuoteAnalyzer
                = Parameter.analyzerParam("search_quote_analyzer", true,
                m -> m.fieldType().getTextSearchInfo().getSearchQuoteAnalyzer(), () -> {
                    if (searchAnalyzer.isConfigured() == false && indexAnalyzer.isConfigured() == false) {
                        NamedAnalyzer defaultAnalyzer = indexAnalyzers.get(AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME);
                        if (defaultAnalyzer != null) {
                            return defaultAnalyzer;
                        }
                    }
                    return searchAnalyzer.get();
                })
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.SEARCH_TIME));
            this.positionIncrementGap = Parameter.intParam("position_increment_gap", false,
                m -> analyzerInitFunction.apply(m).positionIncrementGap.get(), TextFieldMapper.Defaults.POSITION_INCREMENT_GAP)
                .setValidator(v -> {
                    if (v < 0) {
                        throw new MapperParsingException("[position_increment_gap] must be positive, got [" + v + "]");
                    }
                });
            this.indexAnalyzers = indexAnalyzers;
        }

        public NamedAnalyzer getIndexAnalyzer() {
            return wrapAnalyzer(indexAnalyzer.getValue());
        }

        public NamedAnalyzer getSearchAnalyzer() {
            return wrapAnalyzer(searchAnalyzer.getValue());
        }

        public NamedAnalyzer getSearchQuoteAnalyzer() {
            return wrapAnalyzer(searchQuoteAnalyzer.getValue());
        }

        private NamedAnalyzer wrapAnalyzer(NamedAnalyzer a) {
            if (positionIncrementGap.isConfigured() == false) {
                return a;
            }
            return new NamedAnalyzer(a, positionIncrementGap.get());
        }
    }

    public static Parameter<Boolean> norms(boolean defaultValue, Function<FieldMapper, Boolean> initializer) {
        // norms can be updated from 'true' to 'false' but not vv
        return Parameter.boolParam("norms", true, initializer, defaultValue)
            .setMergeValidator((o, n, c) -> o == n || (o && n == false));
    }

    public static Parameter<SimilarityProvider> similarity(Function<FieldMapper, SimilarityProvider> init) {
        return new Parameter<>("similarity", false, () -> null,
            (n, c, o) -> TypeParsers.resolveSimilarity(c, n, o), init)
            .setSerializer((b, f, v) -> b.field(f, v == null ? null : v.name()), v -> v == null ? null : v.name())
            .acceptsNull();
    }

    public static Parameter<String> indexOptions(Function<FieldMapper, String> initializer) {
        return Parameter.restrictedStringParam("index_options", false, initializer,
            "positions", "docs", "freqs", "offsets");
    }

    public static FieldType buildFieldType(Supplier<Boolean> indexed,
                                           Supplier<Boolean> stored,
                                           Supplier<String> indexOptions,
                                           Supplier<Boolean> norms,
                                           Supplier<String> termVectors) {
        FieldType ft = new FieldType();
        ft.setStored(stored.get());
        ft.setTokenized(true);
        ft.setIndexOptions(toIndexOptions(indexed.get(), indexOptions.get()));
        ft.setOmitNorms(norms.get() == false);
        setTermVectorParams(termVectors.get(), ft);
        return ft;
    }

    public static IndexOptions toIndexOptions(boolean indexed, String indexOptions) {
        if (indexed == false) {
            return IndexOptions.NONE;
        }
        switch (indexOptions) {
            case "docs":
                return IndexOptions.DOCS;
            case "freqs":
                return IndexOptions.DOCS_AND_FREQS;
            case "positions":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            case "offsets":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        }
        throw new IllegalArgumentException("Unknown [index_options] value: [" + indexOptions + "]");
    }

    public static Parameter<String> termVectors(Function<FieldMapper, String> initializer) {
        return Parameter.restrictedStringParam("term_vector", false, initializer,
            "no",
            "yes",
            "with_positions",
            "with_offsets",
            "with_positions_offsets",
            "with_positions_payloads",
            "with_positions_offsets_payloads");
    }

    public static void setTermVectorParams(String configuration, FieldType fieldType) {
        switch (configuration) {
            case "no":
                fieldType.setStoreTermVectors(false);
                return;
            case "yes":
                fieldType.setStoreTermVectors(true);
                return;
            case "with_positions":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                return;
            case "with_offsets":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            case "with_positions_offsets":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            case "with_positions_payloads":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
            case "with_positions_offsets_payloads":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
        }
        throw new IllegalArgumentException("Unknown [term_vector] setting: [" + configuration + "]");
    }

}
