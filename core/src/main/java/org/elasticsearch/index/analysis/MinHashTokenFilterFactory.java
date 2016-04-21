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
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;


/**
 *
 */
public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int numHashes;

    public static ParseField NUM_HASHES = new ParseField("num_hashes");

    public static final int DEFAULT_NUM_HASHES = 50;

    public MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.numHashes = settings.getAsInt(NUM_HASHES.getPreferredName(),
            MinHashTokenFilterFactory.DEFAULT_NUM_HASHES);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new MinHashTokenFilter(tokenStream, numHashes);
    }

}
