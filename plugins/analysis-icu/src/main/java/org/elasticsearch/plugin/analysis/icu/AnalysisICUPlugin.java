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

package org.elasticsearch.plugin.analysis.icu;

import static java.util.Collections.singletonMap;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IcuCollationTokenFilterFactory;
import org.elasticsearch.index.analysis.IcuFoldingTokenFilterFactory;
import org.elasticsearch.index.analysis.IcuNormalizerCharFilterFactory;
import org.elasticsearch.index.analysis.IcuNormalizerTokenFilterFactory;
import org.elasticsearch.index.analysis.IcuTokenizerFactory;
import org.elasticsearch.index.analysis.IcuTransformTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.ICUCollationKeywordFieldMapper;
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
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("icu_tokenizer", IcuTokenizerFactory::new);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ICUCollationKeywordFieldMapper.CONTENT_TYPE, new ICUCollationKeywordFieldMapper.TypeParser());
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
