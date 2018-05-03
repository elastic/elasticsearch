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
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class FixedShingleTokenFilterFactory extends AbstractTokenFilterFactory {
    private final int shingleSize;
    private final String tokenSeparator;
    private final String fillerToken;

    public FixedShingleTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.shingleSize = settings.getAsInt("shingle_size", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
        this.tokenSeparator = settings.get("token_separator", ShingleFilter.DEFAULT_TOKEN_SEPARATOR);
        this.fillerToken = settings.get("filler_token", ShingleFilter.DEFAULT_FILLER_TOKEN);
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new FixedShingleFilter(tokenStream, shingleSize, tokenSeparator, fillerToken);
    }
}
