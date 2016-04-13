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

import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
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
        settings = Settings.builder().putArray("stem_exclusion", "foo","bar").build();
        set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));
    }

    public void testParseNonExistingFile() {
        Path tempDir = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("foo.bar_path", tempDir.resolve("foo.dict"))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment env = new Environment(nodeSettings);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Analysis.getWordList(env, nodeSettings, "foo.bar"));
        assertEquals("IOException while reading foo.bar_path: " +  tempDir.resolve("foo.dict").toString(), ex.getMessage());
        assertTrue(ex.getCause().toString(), ex.getCause() instanceof FileNotFoundException
            || ex.getCause() instanceof NoSuchFileException);
    }


    public void testParseFalseEncodedFile() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder()
            .put("foo.bar_path", dict)
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (OutputStream writer = Files.newOutputStream(dict)) {
            writer.write(new byte[]{(byte) 0xff, 0x00, 0x00}); // some invalid UTF-8
            writer.write('\n');
        }
        Environment env = new Environment(nodeSettings);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Analysis.getWordList(env, nodeSettings, "foo.bar"));
        assertEquals("Unsupported character encoding detected while reading foo.bar_path: " + tempDir.resolve("foo.dict").toString()
            + " - files must be UTF-8 encoded" , ex.getMessage());
        assertTrue(ex.getCause().toString(), ex.getCause() instanceof MalformedInputException
            || ex.getCause() instanceof CharacterCodingException);
    }

    public void testParseWordList() throws IOException {
        Path tempDir = createTempDir();
        Path dict = tempDir.resolve("foo.dict");
        Settings nodeSettings = Settings.builder()
            .put("foo.bar_path", dict)
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        try (BufferedWriter writer = Files.newBufferedWriter(dict, StandardCharsets.UTF_8)) {
            writer.write("hello");
            writer.write('\n');
            writer.write("world");
            writer.write('\n');
        }
        Environment env = new Environment(nodeSettings);
        List<String> wordList = Analysis.getWordList(env, nodeSettings, "foo.bar");
        assertEquals(Arrays.asList("hello", "world"), wordList);

    }
}
