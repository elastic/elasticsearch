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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter.Side;
import org.apache.lucene.analysis.ngram.Lucene43EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;


/**
 *
 */
public class EdgeNGramTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int minGram;

    private final int maxGram;

    private final EdgeNGramTokenFilter.Side side;

    private org.elasticsearch.Version esVersion;

    @Inject
    public EdgeNGramTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.minGram = settings.getAsInt("min_gram", NGramTokenFilter.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenFilter.DEFAULT_MAX_NGRAM_SIZE);
        this.side = EdgeNGramTokenFilter.Side.getSide(settings.get("side", Lucene43EdgeNGramTokenizer.DEFAULT_SIDE.getLabel()));
        this.esVersion = org.elasticsearch.Version.indexCreated(indexSettings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (version.onOrAfter(Version.LUCENE_43) && esVersion.onOrAfter(org.elasticsearch.Version.V_0_90_2)) {
            /*
             * We added this in 0.90.2 but 0.90.1 used LUCENE_43 already so we can not rely on the lucene version.
             * Yet if somebody uses 0.90.2 or higher with a prev. lucene version we should also use the deprecated version.
             */
            final Version version = this.version == Version.LUCENE_43 ? Version.LUCENE_44 : this.version; // always use 4.4 or higher
            TokenStream result = tokenStream;
            // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
            if (side == Side.BACK) {
                result = new ReverseStringFilter(version, result);
            }
            result = new EdgeNGramTokenFilter(version, result, minGram, maxGram);
            if (side == Side.BACK) {
                result = new ReverseStringFilter(version, result);
            }
            return result;
        }
        return new EdgeNGramTokenFilter(version, tokenStream, side, minGram, maxGram);
    }
}