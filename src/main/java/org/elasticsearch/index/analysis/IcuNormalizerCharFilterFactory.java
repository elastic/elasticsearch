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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;


/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter} to normalize character.
 * <p/>
 * <p>The <tt>name</tt> can be used to provide the type of normalization to perform.</p>
 * <p>The <tt>mode</tt> can be used to provide 'compose' or 'decompose'. Default is compose.</p>
 */
public class IcuNormalizerCharFilterFactory extends AbstractCharFilterFactory {

    private final String name;

    private final Normalizer2 normalizer;


    @Inject
    public IcuNormalizerCharFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        this.name = settings.get("name", "nfkc_cf");
        String mode = settings.get("mode");
        if (!"compose".equals(mode) && !"decompose".equals(mode)) {
            mode = "compose";
        }
        this.normalizer = Normalizer2.getInstance(
            null, this.name, "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE);
    }

    @Override
    public Reader create(Reader reader) {
        return new ICUNormalizer2CharFilter(reader, normalizer);
    }
}
