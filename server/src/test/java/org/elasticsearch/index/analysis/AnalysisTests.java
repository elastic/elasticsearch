/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class AnalysisTests extends ESTestCase {
    public void testParseStemExclusion() {
        /* Comma separated list */
        Settings settings = Settings.builder().put("stem_exclusion", "foo,bar").build();
        CharArraySet set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));

        /* Array */
        settings = Settings.builder().putList("stem_exclusion", "foo", "bar").build();
        set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));
    }

    public void testParseNonExistingFile() {
        Path tempDir = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("foo.bar_path", tempDir.resolve("foo.dict"))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Analysis.getWordList(env, nodeSettings, "foo.bar")
        );
        assertEquals("IOException while reading foo.bar_path: " + tempDir.resolve("foo.dict").toString(), ex.getMessage());
        assertTrue(
            ex.getCause().toString(),
            ex.getCause() instanceof FileNotFoundException || ex.getCause() instanceof NoSuchFileException
        );
    }

    public void testParseFalseEncodedFile() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (OutputStream writer = Files.newOutputStream(dict)) {
            writer.write(new byte[] { (byte) 0xff, 0x00, 0x00 }); // some invalid UTF-8
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Analysis.getWordList(env, nodeSettings, "foo.bar")
        );
        assertEquals(
            "Unsupported character encoding detected while reading foo.bar_path: "
                + tempDir.resolve("foo.dict").toString()
                + " - files must be UTF-8 encoded",
            ex.getMessage()
        );
        assertTrue(
            ex.getCause().toString(),
            ex.getCause() instanceof MalformedInputException || ex.getCause() instanceof CharacterCodingException
        );
    }

    public void testParseWordList() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder().put("foo.bar_path", dict).put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("hello");
            writer.write('\n');
            writer.write("world");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        List<String> wordList = Analysis.getWordList(env, nodeSettings, "foo.bar");
        assertEquals(Arrays.asList("hello", "world"), wordList);
    }
}
