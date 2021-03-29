/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geoip;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.SuppressForbidden;
import org.junit.Before;
import org.mockito.Matchers;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.util.Constants.WINDOWS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class GeoIpCliTests extends LuceneTestCase {

    private Path tempPath;

    public void setUp() throws Exception {
        super.setUp();
        tempPath = createTempDir();
    }

    @SuppressForbidden(reason = "process builder requires File for directory")
    private File getTempFile() {
        return tempPath.toFile();
    }

    public void test() throws Exception {
        Map<String, byte[]> data = new HashMap<>();
        byte[] a = new byte[514];
        Arrays.fill(a, (byte) 'a');
        Files.write(tempPath.resolve("a.mmdb"), a);
        data.put("a.tgz", a);
        byte[] b = new byte[100];
        Arrays.fill(b, (byte) 'b');
        Files.write(tempPath.resolve("b.mmdb"), b);
        data.put("b.tgz", b);

        Files.createFile(tempPath.resolve("c.tgz"));

        GeoIpCli cli = new GeoIpCli();
        MockTerminal terminal = new MockTerminal();
        OptionSet optionSet = mock(OptionSet.class);
        when(optionSet.valueOf(Matchers.<OptionSpec<String>>anyObject())).thenReturn(tempPath.toAbsolutePath().toString());
        cli.execute(terminal, optionSet);
        Files.delete(tempPath.resolve("a.mmdb"));
        Files.delete(tempPath.resolve("b.mmdb"));
        List<String> files;
        try (Stream<Path> list = Files.list(tempPath)) {
            files = list.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        }
        assertThat(files, containsInAnyOrder("a.tgz", "b.tgz", "c.tgz", "overview.json"));

        // skip tarball verifications on Windows, no tar utility there
        if (WINDOWS) {
            return;
        }

        Map<String, Integer> sizes = Map.of("a.tgz", 514, "b.tgz", 100);
        for (String tgz : List.of("a.tgz", "b.tgz")) {
            String mmdb = tgz.replace(".tgz", ".mmdb");
            Process process = new ProcessBuilder("tar", "-tvf", tgz).directory(getTempFile()).start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                assertThat(line, startsWith("-rw-r--r--"));
                assertThat(line, endsWith(mmdb));
                assertThat(line, matchesRegex(".*1000\\s+1000.*" + sizes.get(tgz) + ".*"));
                assertThat(reader.readLine(), nullValue());
            }
            int exitCode = process.waitFor();
            assertThat(exitCode, is(0));
            process = new ProcessBuilder("tar", "-xzf", tgz).directory(getTempFile()).start();
            exitCode = process.waitFor();
            assertThat(exitCode, is(0));
            assertTrue(Files.exists(tempPath.resolve(mmdb)));
            assertArrayEquals(data.get(tgz), Files.readAllBytes(tempPath.resolve(mmdb)));
        }
    }
}
