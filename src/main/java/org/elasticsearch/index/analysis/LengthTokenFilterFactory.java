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
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.Lucene43LengthFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class LengthTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int min;
    private final int max;
    private final boolean enablePositionIncrements;
    private static final String ENABLE_POS_INC_KEY = "enable_position_increments";

    @Inject
    public LengthTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        min = settings.getAsInt("min", 0);
        max = settings.getAsInt("max", Integer.MAX_VALUE);
        if (version.onOrAfter(Version.LUCENE_4_4) && settings.get(ENABLE_POS_INC_KEY) != null) {
            throw new ElasticsearchIllegalArgumentException(ENABLE_POS_INC_KEY + " is not supported anymore. Please fix your analysis chain or use"
                    + " an older compatibility version (<=4.3) but beware that it might cause highlighting bugs.");
        }
        enablePositionIncrements = version.onOrAfter(Version.LUCENE_4_4) ? true : settings.getAsBoolean(ENABLE_POS_INC_KEY, true);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (version.onOrAfter(Version.LUCENE_4_4)) {
            return new LengthFilter(tokenStream, min, max);
        } else {
            @SuppressWarnings("deprecation")
            final TokenStream filter = new Lucene43LengthFilter(enablePositionIncrements, tokenStream, min, max);
            return filter;
        }
    }
}
