/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.DocValueFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides ICU-based analysis components.
 * This plugin integrates International Components for Unicode (ICU) functionality for text analysis,
 * including normalization, folding, tokenization, and collation support.
 */
public class AnalysisICUPlugin extends Plugin implements AnalysisPlugin, MapperPlugin {

    /**
     * Provides ICU-based character filters for text normalization.
     *
     * @return a map containing the "icu_normalizer" character filter factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * // Configure ICU normalizer character filter in index settings
     * "analysis": {
     *   "char_filter": {
     *     "my_icu_normalizer": {
     *       "type": "icu_normalizer",
     *       "name": "nfc"
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return singletonMap("icu_normalizer", IcuNormalizerCharFilterFactory::new);
    }

    /**
     * Provides ICU-based token filters for text processing.
     * Includes normalization, folding, collation, and transformation filters.
     *
     * @return a map of token filter names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * // Configure ICU token filters in index settings
     * "analysis": {
     *   "filter": {
     *     "my_icu_folding": {
     *       "type": "icu_folding"
     *     },
     *     "my_icu_normalizer": {
     *       "type": "icu_normalizer"
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("icu_normalizer", IcuNormalizerTokenFilterFactory::new);
        extra.put("icu_folding", IcuFoldingTokenFilterFactory::new);
        extra.put("icu_collation", IcuCollationTokenFilterFactory::new);
        extra.put("icu_transform", IcuTransformTokenFilterFactory::new);
        return extra;
    }

    /**
     * Provides the ICU analyzer which combines ICU tokenization, normalization, and folding.
     *
     * @return a map containing the "icu_analyzer" analyzer provider
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * // Configure ICU analyzer in index settings
     * "analysis": {
     *   "analyzer": {
     *     "my_icu_analyzer": {
     *       "type": "icu_analyzer"
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("icu_analyzer", IcuAnalyzerProvider::new);
    }

    /**
     * Provides the ICU tokenizer for Unicode-aware text segmentation.
     *
     * @return a map containing the "icu_tokenizer" tokenizer factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * // Configure ICU tokenizer in index settings
     * "analysis": {
     *   "tokenizer": {
     *     "my_icu_tokenizer": {
     *       "type": "icu_tokenizer"
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("icu_tokenizer", IcuTokenizerFactory::new);
    }

    /**
     * Provides custom field mappers for ICU collation support.
     *
     * @return a map containing the ICU collation keyword field mapper
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * // Define ICU collation keyword field in mappings
     * "mappings": {
     *   "properties": {
     *     "name": {
     *       "type": "icu_collation_keyword",
     *       "language": "en"
     *     }
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ICUCollationKeywordFieldMapper.CONTENT_TYPE, ICUCollationKeywordFieldMapper.PARSER);
    }

    /**
     * Registers named writeable entries for ICU collation format serialization.
     *
     * @return a list of named writeable entries for the collation format
     */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(
            new NamedWriteableRegistry.Entry(
                DocValueFormat.class,
                ICUCollationKeywordFieldMapper.CollationFieldType.COLLATE_FORMAT.getWriteableName(),
                in -> ICUCollationKeywordFieldMapper.CollationFieldType.COLLATE_FORMAT
            )
        );
    }
}
