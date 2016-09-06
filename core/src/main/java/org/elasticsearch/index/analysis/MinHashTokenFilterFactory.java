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
import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.HashMap;
import java.util.Map;


public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {
    private MinHashFilterFactory luceneFactory;

    /**
     * Create a {@link org.apache.lucene.analysis.minhash.MinHashFilterFactory}.
     */
    public MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        // annoying settings indirection because MinHashFilter
        // has package-private constructor and defaults :(
        // see https://issues.apache.org/jira/browse/LUCENE-7436
        Map<String, String> settingsMap = new HashMap<String, String>();
        settingsMap.put("hashCount" ,settings.get("num_hash_tables"));
        settingsMap.put("bucketCount" ,settings.get("num_buckets"));
        settingsMap.put("hashSetSize" ,settings.get("bucket_depth"));
        settingsMap.put("withRotation", settings.get("rotate"));
        luceneFactory = new MinHashFilterFactory(settingsMap);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.analysis.util.TokenFilterFactory#create(org.apache.lucene.analysis.TokenStream)
     */
    @Override
    public TokenStream create(TokenStream input) {
        return luceneFactory.create(input);

    }
}
