/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ArchivePatcher {
    private final Path original;
    private final Path target;
    private final Map<String, Function<? super String, ? extends InputStream>> overrides = new HashMap<>();

    public ArchivePatcher(Path original, Path target) {
        this.original = original;
        this.target = target;
    }

    public void override(String filename, Function<? super String, ? extends InputStream> override) {
        this.overrides.put(filename, override);
    }

    public Path patch() {
        try (
            ZipFile input = new ZipFile(original.toFile());
            ZipOutputStream output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(target.toFile())))
        ) {
            Enumeration<? extends ZipEntry> entries = input.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                output.putNextEntry(entry);
                if (overrides.containsKey(entry.getName())) {
                    Function<? super String, ? extends InputStream> override = overrides.remove(entry.getName());
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input.getInputStream(entry)))) {
                        String content = reader.lines().collect(Collectors.joining(System.lineSeparator()));
                        override.apply(content).transferTo(output);
                    }
                } else {
                    input.getInputStream(entry).transferTo(output);
                }
                output.closeEntry();
            }

            for (Map.Entry<String, Function<? super String, ? extends InputStream>> override : overrides.entrySet()) {
                ZipEntry entry = new ZipEntry(override.getKey());
                output.putNextEntry(entry);
                override.getValue().apply("").transferTo(output);
                output.closeEntry();
            }

            output.flush();
            output.finish();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to patch archive", e);
        }

        return target;
    }
}
