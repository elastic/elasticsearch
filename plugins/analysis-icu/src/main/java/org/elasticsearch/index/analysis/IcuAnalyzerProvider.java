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

package org.elasticsearch.index.analysis;

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.Reader;

public class IcuAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Normalizer2 normalizer;

    public IcuAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        String method = settings.get("method", "nfkc_cf");
        String mode = settings.get("mode", "compose");
        if (!"compose".equals(mode) && !"decompose".equals(mode)) {
            throw new IllegalArgumentException("Unknown mode [" + mode + "] in analyzer [" + name +
                "], expected one of [compose, decompose]");
        }
        Normalizer2 normalizer = Normalizer2.getInstance(
            null, method, "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE);
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(indexSettings, normalizer, settings);
    }

    @Override
    public Analyzer get() {
        return new Analyzer() {

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ICUNormalizer2CharFilter(reader, normalizer);
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new ICUTokenizer();
                return new TokenStreamComponents(source, new ICUFoldingFilter(source));
            }
        };
    }
}
