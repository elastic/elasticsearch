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

import org.elasticsearch.index.analysis.JapaneseStopTokenFilterFactory;
import org.elasticsearch.index.analysis.KuromojiAnalyzerProvider;
import org.elasticsearch.index.analysis.KuromojiBaseFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiIterationMarkCharFilterFactory;
import org.elasticsearch.index.analysis.KuromojiKatakanaStemmerFactory;
import org.elasticsearch.index.analysis.KuromojiNumberFilterFactory;
import org.elasticsearch.index.analysis.KuromojiPartOfSpeechFilterFactory;
import org.elasticsearch.index.analysis.KuromojiReadingFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.Plugin;

/**
 *
 */
public class AnalysisKuromojiPlugin extends Plugin {

    public void onModule(AnalysisModule module) {
        module.registerCharFilter("kuromoji_iteration_mark", KuromojiIterationMarkCharFilterFactory::new);
        module.registerAnalyzer("kuromoji", KuromojiAnalyzerProvider::new);
        module.registerTokenizer("kuromoji_tokenizer", KuromojiTokenizerFactory::new);
        module.registerTokenFilter("kuromoji_baseform", KuromojiBaseFormFilterFactory::new);
        module.registerTokenFilter("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory::new);
        module.registerTokenFilter("kuromoji_readingform", KuromojiReadingFormFilterFactory::new);
        module.registerTokenFilter("kuromoji_stemmer", KuromojiKatakanaStemmerFactory::new);
        module.registerTokenFilter("ja_stop", JapaneseStopTokenFilterFactory::new);
        module.registerTokenFilter("kuromoji_number", KuromojiNumberFilterFactory::new);
    }
}
