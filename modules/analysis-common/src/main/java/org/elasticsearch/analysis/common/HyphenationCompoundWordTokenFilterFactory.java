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
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.xml.sax.InputSource;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Uses the {@link org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter} to decompound tokens based on hyphenation rules.
 *
 * @see org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter
 */
public class HyphenationCompoundWordTokenFilterFactory extends AbstractCompoundWordTokenFilterFactory {

    private final HyphenationTree hyphenationTree;

    HyphenationCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);

        String hyphenationPatternsPath = settings.get("hyphenation_patterns_path", null);
        if (hyphenationPatternsPath == null) {
            throw new IllegalArgumentException("hyphenation_patterns_path is a required setting.");
        }

        Path hyphenationPatternsFile = env.configFile().resolve(hyphenationPatternsPath);

        try {
            InputStream in = Files.newInputStream(hyphenationPatternsFile);
            hyphenationTree = HyphenationCompoundWordTokenFilter.getHyphenationTree(new InputSource(in));
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while reading hyphenation_patterns_path.", e);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HyphenationCompoundWordTokenFilter(tokenStream, hyphenationTree, wordList, minWordSize,
                                                      minSubwordSize, maxSubwordSize, onlyLongestMatch);
    }
}
