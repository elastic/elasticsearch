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
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

/**
 * Factory for ASCIIFoldingFilter.
 */
public class ASCIIFoldingTokenFilterFactory extends AbstractTokenFilterFactory
        implements NormalizingTokenFilterFactory {

    public static final ParseField PRESERVE_ORIGINAL = new ParseField("preserve_original");
    public static final boolean DEFAULT_PRESERVE_ORIGINAL = false;

    private final boolean preserveOriginal;

    public ASCIIFoldingTokenFilterFactory(IndexSettings indexSettings, Environment environment,
            String name, Settings settings) {
        super(indexSettings, name, settings);
        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIGINAL.getPreferredName(), DEFAULT_PRESERVE_ORIGINAL);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ASCIIFoldingFilter(tokenStream, preserveOriginal);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (preserveOriginal == false) {
            return this;
        } else {
            // See https://issues.apache.org/jira/browse/LUCENE-7536 for the reasoning
            return new TokenFilterFactory() {
                @Override
                public String name() {
                    return ASCIIFoldingTokenFilterFactory.this.name();
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return new ASCIIFoldingFilter(tokenStream, false);
                }
            };
        }
    }

    public TokenStream normalize(TokenStream tokenStream) {
        // Normalization should only emit a single token, so always turn off preserveOriginal
        return new ASCIIFoldingFilter(tokenStream, false);
    }

}
