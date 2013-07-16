/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.analysis.pattern.XPatternCaptureGroupTokenFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.regex.Pattern;

@AnalysisSettingsRequired
public class PatternCaptureGroupTokenFilterFactory extends AbstractTokenFilterFactory {
    private Pattern[] patterns;
    private boolean preserveOriginal;

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1471347.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }


    @Inject
    public PatternCaptureGroupTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name,
            @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        String[] regexes = settings.getAsArray("patterns",Strings.EMPTY_ARRAY,false);
        patterns = new Pattern[regexes.length];
        for (int i = 0; i < regexes.length; i++) {
            patterns[i] = Pattern.compile(regexes[i]);
        }

        preserveOriginal = settings.getAsBoolean("preserve_original", true);
    }

    @Override
    public XPatternCaptureGroupTokenFilter create(TokenStream tokenStream) {
        return new XPatternCaptureGroupTokenFilter(tokenStream, preserveOriginal, patterns);
    }
}
