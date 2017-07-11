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

import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.Reader;


/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter} to normalize character.
 * <p>The <tt>name</tt> can be used to provide the type of normalization to perform.</p>
 * <p>The <tt>mode</tt> can be used to provide 'compose' or 'decompose'. Default is compose.</p>
 * <p>The <tt>unicodeSetFilter</tt> attribute can be used to provide the UniCodeSet for filtering.</p>
 */
public class IcuNormalizerCharFilterFactory extends AbstractCharFilterFactory implements MultiTermAwareComponent {

    private final Normalizer2 normalizer;

    public IcuNormalizerCharFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name);
        String method = settings.get("name", "nfkc_cf");
        String mode = settings.get("mode");
        if (!"compose".equals(mode) && !"decompose".equals(mode)) {
            mode = "compose";
        }
        Normalizer2 normalizer = Normalizer2.getInstance(
            null, method, "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE);
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(normalizer, settings);
    }

    @Override
    public Reader create(Reader reader) {
        return new ICUNormalizer2CharFilter(reader, normalizer);
    }

    @Override
    public Object getMultiTermComponent() {
        return this;
    }
}
