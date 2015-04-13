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

package org.elasticsearch.index.analysis.compound;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.Lucene43HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.util.Version;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisSettingsRequired;
import org.elasticsearch.index.settings.IndexSettings;
import org.xml.sax.InputSource;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Uses the {@link org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter} to decompound tokens based on hyphenation rules.
 *
 * @see org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter
 */
@AnalysisSettingsRequired
public class HyphenationCompoundWordTokenFilterFactory extends AbstractCompoundWordTokenFilterFactory {

    private final HyphenationTree hyphenationTree;

    @Inject
    public HyphenationCompoundWordTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, env, name, settings);

        String hyphenationPatternsPath = settings.get("hyphenation_patterns_path", null);
        if (hyphenationPatternsPath == null) {
            throw new ElasticsearchIllegalArgumentException("hyphenation_patterns_path is a required setting.");
        }

        URL hyphenationPatternsFile = env.resolveConfig(hyphenationPatternsPath);

        try {
            hyphenationTree = HyphenationCompoundWordTokenFilter.getHyphenationTree(new InputSource(hyphenationPatternsFile.toExternalForm()));
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("Exception while reading hyphenation_patterns_path: " + e.getMessage());
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (version.onOrAfter(Version.LUCENE_4_4_0)) {
            return new HyphenationCompoundWordTokenFilter(tokenStream, hyphenationTree, wordList, minWordSize, 
                                                          minSubwordSize, maxSubwordSize, onlyLongestMatch);
        } else {
            return new Lucene43HyphenationCompoundWordTokenFilter(tokenStream, hyphenationTree, wordList, minWordSize, 
                    minSubwordSize, maxSubwordSize, onlyLongestMatch);
        }
    }
}