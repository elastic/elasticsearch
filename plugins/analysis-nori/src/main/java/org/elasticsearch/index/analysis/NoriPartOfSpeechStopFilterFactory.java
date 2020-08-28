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
import org.apache.lucene.analysis.ko.KoreanPartOfSpeechStopFilter;
import org.apache.lucene.analysis.ko.POS;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NoriPartOfSpeechStopFilterFactory extends AbstractTokenFilterFactory {
    private final Set<POS.Tag> stopTags;

    public NoriPartOfSpeechStopFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        List<String> tagList = Analysis.getWordList(env, settings, "stoptags");
        this.stopTags = tagList != null ? resolvePOSList(tagList) : KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new KoreanPartOfSpeechStopFilter(tokenStream, stopTags);
    }


    static Set<POS.Tag> resolvePOSList(List<String> tagList) {
        Set<POS.Tag> stopTags = new HashSet<>();
        for (String tag : tagList) {
            stopTags.add(POS.resolveTag(tag));
        }
        return stopTags;
    }
}
