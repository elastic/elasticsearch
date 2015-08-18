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

package org.elasticsearch.plugin.analysis.kuromoji;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.JapaneseStopTokenFilterFactory;
import org.elasticsearch.index.analysis.KuromojiAnalyzerProvider;
import org.elasticsearch.index.analysis.KuromojiBaseFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiIterationMarkCharFilterFactory;
import org.elasticsearch.index.analysis.KuromojiKatakanaStemmerFactory;
import org.elasticsearch.index.analysis.KuromojiPartOfSpeechFilterFactory;
import org.elasticsearch.index.analysis.KuromojiReadingFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiTokenizerFactory;
import org.elasticsearch.indices.analysis.KuromojiIndicesAnalysisModule;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AnalysisKuromojiPlugin extends Plugin {

    @Override
    public String name() {
        return "analysis-kuromoji";
    }

    @Override
    public String description() {
        return "Kuromoji analysis support";
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new KuromojiIndicesAnalysisModule());
    }

    public void onModule(AnalysisModule module) {
        module.addCharFilter("kuromoji_iteration_mark", KuromojiIterationMarkCharFilterFactory.class);
        module.addAnalyzer("kuromoji", KuromojiAnalyzerProvider.class);
        module.addTokenizer("kuromoji_tokenizer", KuromojiTokenizerFactory.class);
        module.addTokenFilter("kuromoji_baseform", KuromojiBaseFormFilterFactory.class);
        module.addTokenFilter("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory.class);
        module.addTokenFilter("kuromoji_readingform", KuromojiReadingFormFilterFactory.class);
        module.addTokenFilter("kuromoji_stemmer", KuromojiKatakanaStemmerFactory.class);
        module.addTokenFilter("ja_stop", JapaneseStopTokenFilterFactory.class);
    }
}
