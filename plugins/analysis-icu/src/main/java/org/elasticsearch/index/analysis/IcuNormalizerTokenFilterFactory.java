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

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;


/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2Filter} to normalize tokens.
 * <p/>
 * <p>The <tt>name</tt> can be used to provide the type of normalization to perform.
 *
 *
 */
public class IcuNormalizerTokenFilterFactory extends AbstractTokenFilterFactory {

    private final String name;

    @Inject
    public IcuNormalizerTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        this.name = settings.get("name", "nfkc_cf");
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, Normalizer2.getInstance(null, name, Normalizer2.Mode.COMPOSE));
    }
}
