/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geoip;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class GeoIpCliTests extends LuceneTestCase {

    private Path source;
    private Path target;

    public void setUp() throws Exception {
        super.setUp();
        Path tempPath = createTempDir();
        source = tempPath.resolve("source");
        target = tempPath.resolve("target");
        Files.createDirectory(source);
        Files.createDirectory(target);
    }

    @SuppressForbidden(reason = "process builder requires File for directory")
    private File getTargetFile() {
        return target.toFile();
    }

    public void testNoSource() throws Exception {
        MockTerminal terminal = new MockTerminal();
        new GeoIpCli().main(new String[] {}, terminal);
        assertThat(terminal.getErrorOutput(), containsString("Missing required option(s) [s/source]"));
    }

    public void testDifferentDirectories() throws Exception {
        Map<String, byte[]> data = createTestFiles(source);

        GeoIpCli cli = new GeoIpCli();
        cli.main(new String[] { "-t", target.toAbsolutePath().toString(), "-s", source.toAbsolutePath().toString() }, new MockTerminal());

        try (Stream<Path> list = Files.list(source)) {
            List<String> files = list.map(p -> p.getFileName().toString()).collect(Collectors.toList());
            assertThat(files, containsInAnyOrder("a.mmdb", "b.mmdb", "c.tgz"));
        }

        try (Stream<Path> list = Files.list(target)) {
            List<String> files = list.map(p -> p.getFileName().toString()).collect(Collectors.toList());
            assertThat(files, containsInAnyOrder("a.tgz", "b.tgz", "c.tgz", "overview.json"));
        }

        verifyTarball(data);
        verifyOverview();
    }

    public void testSameDirectory() throws Exception {
        Map<String, byte[]> data = createTestFiles(target);

        GeoIpCli cli = new GeoIpCli();
        cli.main(new String[] { "-s", target.toAbsolutePath().toString() }, new MockTerminal());

        try (Stream<Path> list = Files.list(target)) {
            List<String> files = list.map(p -> p.getFileName().toString()).collect(Collectors.toList());
            assertThat(files, containsInAnyOrder("a.mmdb", "b.mmdb", "a.tgz", "b.tgz", "c.tgz", "overview.json"));
        }

        Files.delete(target.resolve("a.mmdb"));
        Files.delete(target.resolve("b.mmdb"));

        verifyTarball(data);
        verifyOverview();
    }

    private void verifyOverview() throws Exception {
        byte[] data = Files.readAllBytes(target.resolve("overview.json"));
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, data)
        ) {
            @SuppressWarnings({ "unchecked" })
            List<Map<String, String>> list = (List) parser.list();
            assertThat(list, containsInAnyOrder(hasEntry("name", "a.tgz"), hasEntry("name", "b.tgz"), hasEntry("name", "c.tgz")));
            assertThat(list, containsInAnyOrder(hasEntry("url", "a.tgz"), hasEntry("url", "b.tgz"), hasEntry("url", "c.tgz")));
            for (Map<String, String> map : list) {
                assertThat(map, hasKey("md5_hash"));
                assertThat(map, hasKey("updated"));
            }
        }
    }

    private void verifyTarball(Map<String, byte[]> data) throws Exception {
        // skip tarball verifications on Windows, no tar utility there
        if (WINDOWS) {
            return;
        }

        Map<String, Integer> sizes = Map.of("a.tgz", 514, "b.tgz", 100);
        for (String tgz : List.of("a.tgz", "b.tgz")) {
            String mmdb = tgz.replace(".tgz", ".mmdb");
            Process process = new ProcessBuilder("tar", "-tvf", tgz).directory(getTargetFile()).start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                assertThat(line, startsWith("-rw-r--r--"));
                assertThat(line, endsWith(mmdb));
                assertThat(line, matchesRegex(".*1000\\s+1000.*" + sizes.get(tgz) + ".*"));
                assertThat(reader.readLine(), nullValue());
            }
            int exitCode = process.waitFor();
            assertThat(exitCode, is(0));
            process = new ProcessBuilder("tar", "-xzf", tgz).directory(getTargetFile()).start();
            exitCode = process.waitFor();
            assertThat(exitCode, is(0));
            assertTrue(Files.exists(target.resolve(mmdb)));
            assertArrayEquals(data.get(tgz), Files.readAllBytes(target.resolve(mmdb)));
        }
    }

    private Map<String, byte[]> createTestFiles(Path dir) throws IOException {
        Map<String, byte[]> data = new HashMap<>();

        byte[] a = new byte[514];
        Arrays.fill(a, (byte) 'a');
        Files.write(dir.resolve("a.mmdb"), a);
        data.put("a.tgz", a);

        byte[] b = new byte[100];
        Arrays.fill(b, (byte) 'b');
        Files.write(dir.resolve("b.mmdb"), b);
        data.put("b.tgz", b);

        Files.createFile(dir.resolve("c.tgz"));

        return data;
    }
}
