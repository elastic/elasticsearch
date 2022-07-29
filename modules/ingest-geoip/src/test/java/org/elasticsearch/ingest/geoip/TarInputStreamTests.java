/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TarInputStreamTests extends ESTestCase {

    private final String path;
    private final List<Entry> entries;

    public TarInputStreamTests(@Name("path") String path, @Name("entries") List<Entry> entries) {
        this.path = path;
        this.entries = entries;
    }

    public void test() throws IOException {
        try (InputStream is = TarInputStreamTests.class.getResourceAsStream(path); TarInputStream tis = new TarInputStream(is)) {
            assertNotNull(is);
            for (Entry entry : entries) {
                TarInputStream.TarEntry tarEntry = tis.getNextEntry();
                assertEquals(entry.name, tarEntry.name());
                if (entry.notFile == false) {
                    assertEquals(entry.data, new String(tis.readAllBytes(), StandardCharsets.UTF_8));
                }
                assertEquals(entry.notFile, tarEntry.notFile());
            }
            assertNull(tis.getNextEntry());
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        Object[][] entries = new Object[][] {
            createTest("tar1.tar", new Entry("a.txt", "aaa\n", false)),
            createTest("tar2.tar", new Entry("a.txt", "aaa\n", false), new Entry("b.txt", "bbbbbb\n", false)),
            createTest(
                "tar3.tar",
                new Entry("c.txt", Stream.generate(() -> "-").limit(512).collect(Collectors.joining()), false),
                new Entry("b.txt", "bbbbbb\n", false)
            ),
            createTest(
                "tar4.tar",
                new Entry("./", null, true),
                new Entry("./b.txt", "bbb\n", false),
                new Entry("./a.txt", "aaa\n", false)
            ) };
        return Arrays.asList(entries);
    }

    private static Object[] createTest(String name, Entry... entries) {
        return new Object[] { name, Arrays.asList(entries) };
    }

    private static class Entry {
        String name;
        String data;
        boolean notFile;

        private Entry(String name, String data, boolean notFile) {
            this.name = name;
            this.data = data;
            this.notFile = notFile;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
