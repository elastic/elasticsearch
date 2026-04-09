/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class PathHierarchyTokenizerFactoryTests extends ESTokenStreamTestCase {

    public void testDefaults() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            Settings.EMPTY
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one", "/one/two", "/one/two/three" });
    }

    public void testReverse() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("reverse", true).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one/two/three", "one/two/three", "two/three", "three" });
    }

    public void testDelimiter() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("delimiter", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one/two/three" });
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] { "one", "one-two", "one-two-three" });
    }

    public void testReplace() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("replacement", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "-one", "-one-two", "-one-two-three" });
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] { "one-two-three" });
    }

    public void testSkip() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("skip", 2).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three/four/five"));
        assertTokenStreamContents(tokenizer, new String[] { "/three", "/three/four", "/three/four/five" });
    }

    public void testDelimiterExceptions() {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        {
            String delimiter = RandomPicks.randomFrom(random(), new String[] { "--", "" });
            Settings settings = newAnalysisSettingsBuilder().put("delimiter", delimiter).build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new PathHierarchyTokenizerFactory(
                    IndexSettingsModule.newIndexSettings(index, indexSettings),
                    null,
                    "path-hierarchy-tokenizer",
                    settings
                ).create()
            );
            assertEquals("delimiter must be a one char value", e.getMessage());
        }
        {
            String replacement = RandomPicks.randomFrom(random(), new String[] { "--", "" });
            Settings settings = newAnalysisSettingsBuilder().put("replacement", replacement).build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new PathHierarchyTokenizerFactory(
                    IndexSettingsModule.newIndexSettings(index, indexSettings),
                    null,
                    "path-hierarchy-tokenizer",
                    settings
                ).create()
            );
            assertEquals("replacement must be a one char value", e.getMessage());
        }
    }
}
