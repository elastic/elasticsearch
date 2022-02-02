/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

public class AnalysisICUPlugin extends Plugin implements AnalysisPlugin, MapperPlugin {
    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return singletonMap("icu_normalizer", IcuNormalizerCharFilterFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("icu_normalizer", IcuNormalizerTokenFilterFactory::new);
        extra.put("icu_folding", IcuFoldingTokenFilterFactory::new);
        extra.put("icu_collation", IcuCollationTokenFilterFactory::new);
        extra.put("icu_transform", IcuTransformTokenFilterFactory::new);
        return extra;
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("icu_analyzer", IcuAnalyzerProvider::new);
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("icu_tokenizer", IcuTokenizerFactory::new);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ICUCollationKeywordFieldMapper.CONTENT_TYPE, ICUCollationKeywordFieldMapper.PARSER);
    }

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
