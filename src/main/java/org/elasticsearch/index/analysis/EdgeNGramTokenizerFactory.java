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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.Lucene43EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;

import static org.elasticsearch.index.analysis.NGramTokenizerFactory.parseTokenChars;

/**
 *
 */
@SuppressWarnings("deprecation")
public class EdgeNGramTokenizerFactory extends AbstractTokenizerFactory {

    private final int minGram;

    private final int maxGram;

    private final Lucene43EdgeNGramTokenizer.Side side;

    private final CharMatcher matcher;
    
    protected org.elasticsearch.Version esVersion;


    @Inject
    public EdgeNGramTokenizerFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.minGram = settings.getAsInt("min_gram", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("max_gram", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
        this.side = Lucene43EdgeNGramTokenizer.Side.getSide(settings.get("side", Lucene43EdgeNGramTokenizer.DEFAULT_SIDE.getLabel()));
        this.matcher = parseTokenChars(settings.getAsArray("token_chars"));
        this.esVersion = org.elasticsearch.Version.indexCreated(indexSettings);
    }

    @Override
    public Tokenizer create() {
        if (version.onOrAfter(Version.LUCENE_4_3) && esVersion.onOrAfter(org.elasticsearch.Version.V_0_90_2)) {
            /*
             * We added this in 0.90.2 but 0.90.1 used LUCENE_43 already so we can not rely on the lucene version.
             * Yet if somebody uses 0.90.2 or higher with a prev. lucene version we should also use the deprecated version.
             */
            if (side == Lucene43EdgeNGramTokenizer.Side.BACK) {
                throw new ElasticsearchIllegalArgumentException("side=back is not supported anymore. Please fix your analysis chain or use"
                        + " an older compatibility version (<=4.2) but beware that it might cause highlighting bugs." 
                        + " To obtain the same behavior as the previous version please use \"edgeNGram\" filter which still supports side=back" 
                        + " in combination with a \"keyword\" tokenizer");
            }
            final Version version = this.version == Version.LUCENE_4_3 ? Version.LUCENE_4_4 : this.version; // always use 4.4 or higher
            if (matcher == null) {
                return new EdgeNGramTokenizer(minGram, maxGram);
            } else {
                return new EdgeNGramTokenizer(minGram, maxGram) {
                    @Override
                    protected boolean isTokenChar(int chr) {
                        return matcher.isTokenChar(chr);
                    }
                };
            }
        } else {
            return new Lucene43EdgeNGramTokenizer(side, minGram, maxGram);
        }
    }
}