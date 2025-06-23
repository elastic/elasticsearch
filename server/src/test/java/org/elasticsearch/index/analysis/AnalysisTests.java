/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

import static org.hamcrest.CoreMatchers.containsString;
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

    public void testParseDuplicates() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder()
            .put("foo.path", tempDir.resolve(dict))
            .put("bar.list", "")
            .put("soup.lenient", "true")
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞,extra stuff that gets discarded");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        List<String> wordList = Analysis.getWordList(env, nodeSettings, "foo.path", "bar.list", "soup.lenient", true, true);
        assertEquals(List.of("最終契約,最終契約,最終契約,カスタム名 詞"), wordList);
    }

    public void testFailOnDuplicates() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder()
            .put("foo.path", tempDir.resolve(dict))
            .put("bar.list", "")
            .put("soup.lenient", "false")
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("最終契,最終契,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞,extra");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> Analysis.getWordList(env, nodeSettings, "foo.path", "bar.list", "soup.lenient", false, true)
        );
        assertThat(exc.getMessage(), containsString("[最終契約] in user dictionary at line [5]"));
    }

    public void testParseDuplicatesWComments() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder()
            .put("foo.path", tempDir.resolve(dict))
            .put("bar.list", "")
            .put("soup.lenient", "true")
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞");
            writer.write('\n');
            writer.write("# This is a test of the emergency broadcast system");
            writer.write('\n');
            writer.write("最終契約,最終契約,最終契約,カスタム名 詞,extra");
            writer.write('\n');
        }
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        List<String> wordList = Analysis.getWordList(env, nodeSettings, "foo.path", "bar.list", "soup.lenient", false, true);
        assertEquals(
            List.of(
                "# This is a test of the emergency broadcast system",
                "最終契約,最終契約,最終契約,カスタム名 詞",
                "# This is a test of the emergency broadcast system"
            ),
            wordList
        );
    }
}
