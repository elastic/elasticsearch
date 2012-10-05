/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsQueryFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Set;

/**
 *
 */
public class CommonGramsQueryTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Set<?> words;

    private final boolean ignoreCase;

    @Inject
    public CommonGramsQueryTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.words = Analysis.parseCommonWords(env, settings, StopAnalyzer.ENGLISH_STOP_WORDS_SET, version, ignoreCase);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        CommonGramsFilter commonGrams = new CommonGramsFilter(version, tokenStream, words);
        CommonGramsQueryFilter filter = new CommonGramsQueryFilter(commonGrams);
        return filter;
    }

    public Set<?> words() {
        return words;
    }

    public boolean ignoreCase() {
        return ignoreCase;
    }

}
