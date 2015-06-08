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

import org.apache.lucene.analysis.miscellaneous.Lucene43TrimFilter;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class TrimTokenFilterFactory extends AbstractTokenFilterFactory {

    private final boolean updateOffsets;
    private static final String UPDATE_OFFSETS_KEY = "update_offsets";

    @Inject
    public TrimTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        if (version.onOrAfter(Version.LUCENE_4_4_0) && settings.get(UPDATE_OFFSETS_KEY) != null) {
            throw new IllegalArgumentException(UPDATE_OFFSETS_KEY +  " is not supported anymore. Please fix your analysis chain or use"
                    + " an older compatibility version (<=4.3) but beware that it might cause highlighting bugs.");
        }
        this.updateOffsets = settings.getAsBoolean("update_offsets", false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (version.onOrAfter(Version.LUCENE_4_4_0)) {
            return new TrimFilter(tokenStream);
        } else {
            @SuppressWarnings("deprecation")
            final TokenStream filter = new Lucene43TrimFilter(tokenStream, updateOffsets);
            return filter;
        }
    }
}
