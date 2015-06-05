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

package org.elasticsearch.index.analysis.pl;

import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.stempel.StempelFilter;
import org.apache.lucene.analysis.stempel.StempelStemmer;
import org.egothor.stemmer.Trie;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import java.io.IOException;



public class PolishStemTokenFilterFactory extends AbstractTokenFilterFactory {

    private final StempelStemmer stemmer;

    @Inject public PolishStemTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        Trie tire;
        try {
            tire = StempelStemmer.load(PolishAnalyzer.class.getResourceAsStream(PolishAnalyzer.DEFAULT_STEMMER_FILE));
        } catch (IOException ex) {
            throw new RuntimeException("Unable to load default stemming tables", ex);
        }
        stemmer = new StempelStemmer(tire);
    }

    @Override public TokenStream create(TokenStream tokenStream) {
        return new StempelFilter(tokenStream, stemmer);
    }
}
