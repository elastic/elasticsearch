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

package org.elasticsearch.indices.analysis;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.Transliterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.icu.ICUTransformFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.ICUCollationKeyFilter;
import org.elasticsearch.index.analysis.PreBuiltCharFilterFactoryFactory;
import org.elasticsearch.index.analysis.PreBuiltTokenFilterFactoryFactory;
import org.elasticsearch.index.analysis.PreBuiltTokenizerFactoryFactory;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

import java.io.Reader;

/**
 * Registers indices level analysis components so, if not explicitly configured, will be shared
 * among all indices.
 */
public class IcuIndicesAnalysis extends AbstractComponent {

    @Inject
    public IcuIndicesAnalysis(Settings settings, IndicesAnalysisService indicesAnalysisService) {
        super(settings);

        indicesAnalysisService.tokenizerFactories().put("icu_tokenizer", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "icu_tokenizer";
            }

            @Override
            public Tokenizer create() {
                return new ICUTokenizer();
            }
        }));

        indicesAnalysisService.tokenFilterFactories().put("icu_normalizer", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "icu_normalizer";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
            }
        }));


        indicesAnalysisService.tokenFilterFactories().put("icu_folding", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "icu_folding";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ICUFoldingFilter(tokenStream);
            }
        }));

        indicesAnalysisService.tokenFilterFactories().put("icu_collation", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "icu_collation";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ICUCollationKeyFilter(tokenStream, Collator.getInstance());
            }
        }));

        indicesAnalysisService.tokenFilterFactories().put("icu_transform", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "icu_transform";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ICUTransformFilter(tokenStream, Transliterator.getInstance("Null", Transliterator.FORWARD));
            }
        }));
        
        indicesAnalysisService.charFilterFactories().put("icu_normalizer", new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
            @Override
            public String name() {
                return "icu_normalizer";
            }

            @Override
            public Reader create(Reader reader) {
                return new ICUNormalizer2CharFilter(reader);
            }
        }));
    }
}
