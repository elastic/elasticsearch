/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class WhitespaceTokenizerFactoryTests extends ESTestCase {

    public void testSimpleWhiteSpaceTokenizer() throws IOException {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), indexSettings);
        WhitespaceTokenizer tokenizer = (WhitespaceTokenizer) new WhitespaceTokenizerFactory(
            indexProperties,
            null,
            "whitespace_maxlen",
            Settings.EMPTY
        ).create();

        try (Reader reader = new StringReader("one, two, three")) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { "one,", "two,", "three" });
        }
    }

    public void testMaxTokenLength() throws IOException {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), indexSettings);
        final Settings settings = Settings.builder().put(WhitespaceTokenizerFactory.MAX_TOKEN_LENGTH, 2).build();
        WhitespaceTokenizer tokenizer = (WhitespaceTokenizer) new WhitespaceTokenizerFactory(
            indexProperties,
            null,
            "whitespace_maxlen",
            settings
        ).create();
        try (Reader reader = new StringReader("one, two, three")) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { "on", "e,", "tw", "o,", "th", "re", "e" });
        }

        final Settings defaultSettings = Settings.EMPTY;
        tokenizer = (WhitespaceTokenizer) new WhitespaceTokenizerFactory(indexProperties, null, "whitespace_maxlen", defaultSettings)
            .create();
        String veryLongToken = RandomStrings.randomAsciiAlphanumOfLength(random(), 256);
        try (Reader reader = new StringReader(veryLongToken)) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { veryLongToken.substring(0, 255), veryLongToken.substring(255) });
        }

        final Settings tooLongSettings = Settings.builder().put(WhitespaceTokenizerFactory.MAX_TOKEN_LENGTH, 1024 * 1024 + 1).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new WhitespaceTokenizerFactory(indexProperties, null, "whitespace_maxlen", tooLongSettings).create()
        );
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 1048577", e.getMessage());

        final Settings negativeSettings = Settings.builder().put(WhitespaceTokenizerFactory.MAX_TOKEN_LENGTH, -1).build();
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new WhitespaceTokenizerFactory(indexProperties, null, "whitespace_maxlen", negativeSettings).create()
        );
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: -1", e.getMessage());
    }
}
