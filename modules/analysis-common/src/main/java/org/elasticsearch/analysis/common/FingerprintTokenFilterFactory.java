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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import static org.elasticsearch.analysis.common.FingerprintAnalyzerProvider.DEFAULT_MAX_OUTPUT_SIZE;
import static org.elasticsearch.analysis.common.FingerprintAnalyzerProvider.MAX_OUTPUT_SIZE;

public class FingerprintTokenFilterFactory extends AbstractTokenFilterFactory {

    private final char separator;
    private final int maxOutputSize;

    FingerprintTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.separator = FingerprintAnalyzerProvider.parseSeparator(settings);
        this.maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(), DEFAULT_MAX_OUTPUT_SIZE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;
        result = new FingerprintFilter(result, maxOutputSize, separator);
        return result;
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }

}
