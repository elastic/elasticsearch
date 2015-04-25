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

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class SentenceTokenizerFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testSentenceTokenizer() throws IOException {
        final String text = "Hello world! How are you? I am fine.\n" +
                "This is a difficult sentence because I use I.D.\n" +
                "\n" +
                "Newlines should also be accepted. Numbers should not cause \n" +
                "sentence breaks, like 1.23.";
        final Index index = new Index("test");
        final String name = "sent";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = ImmutableSettings.EMPTY;
        Tokenizer tokenizer = new SentenceTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(text));
        assertTokenStreamContents(tokenizer, new String[]{
                "Hello world! ",
                "How are you? ",
                "I am fine.\n",
                "This is a difficult sentence because I use I.D.\n\n",
                "Newlines should also be accepted. ",
                "Numbers should not cause \nsentence breaks, like 1.23."});
    }

    @Test
    public void testSentenceTokenizerEmpty() throws IOException {
        final String text = "";
        final Index index = new Index("test");
        final String name = "sent";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = ImmutableSettings.EMPTY;
        Tokenizer tokenizer = new SentenceTokenizerFactory(index, indexSettings, name, settings).create(new StringReader(text));
        assertTokenStreamContents(tokenizer, new String[]{});
    }
}
