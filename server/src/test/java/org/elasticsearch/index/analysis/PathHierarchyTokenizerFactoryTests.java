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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.StringReader;

public class PathHierarchyTokenizerFactoryTests extends ESTokenStreamTestCase {

    public void testDefaults() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                "path-hierarchy-tokenizer", Settings.EMPTY).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] {"/one", "/one/two", "/one/two/three"});
    }

    public void testReverse() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("reverse", true).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                "path-hierarchy-tokenizer", settings).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] {"/one/two/three", "one/two/three", "two/three", "three"});
    }

    public void testDelimiter() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("delimiter", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                "path-hierarchy-tokenizer", settings).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] {"/one/two/three"});
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] {"one", "one-two", "one-two-three"});
    }

    public void testReplace() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("replacement", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                "path-hierarchy-tokenizer", settings).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] {"-one", "-one-two", "-one-two-three"});
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] {"one-two-three"});
    }

    public void testSkip() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("skip", 2).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                "path-hierarchy-tokenizer", settings).create();
        tokenizer.setReader(new StringReader("/one/two/three/four/five"));
        assertTokenStreamContents(tokenizer, new String[] {"/three", "/three/four", "/three/four/five"});
    }

    public void testDelimiterExceptions() {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        {
            String delimiter = RandomPicks.randomFrom(random(), new String[] {"--", ""});
            Settings settings = newAnalysisSettingsBuilder().put("delimiter", delimiter).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                            "path-hierarchy-tokenizer", settings).create());
            assertEquals("delimiter must be a one char value", e.getMessage());
        }
        {
            String replacement = RandomPicks.randomFrom(random(), new String[] {"--", ""});
            Settings settings = newAnalysisSettingsBuilder().put("replacement", replacement).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> new PathHierarchyTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null,
                            "path-hierarchy-tokenizer", settings).create());
            assertEquals("replacement must be a one char value", e.getMessage());
        }
    }
}
