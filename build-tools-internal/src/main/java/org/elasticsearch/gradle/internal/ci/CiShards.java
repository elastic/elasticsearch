/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * The stable hash used to place things the checked-in CI part assignment does not know about yet
 * (new test tasks, new projects) and to split a test task into shards.
 *
 * <p>CRC32 over UTF-8 is used instead of {@link String#hashCode()} because it mixes the low bits
 * better for similar names (e.g. {@code FooIT}/{@code FooTests}), and it is fully specified, so a
 * name maps to the same bucket on every machine and JDK. {@code ESClientYamlSuiteTestCase} carries
 * a copy of {@link #bucket} for splitting YAML suites; keep them identical.
 */
public final class CiShards {

    private CiShards() {}

    /** Returns a bucket in {@code [0, count)} for {@code name}. */
    public static int bucket(String name, int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive but was " + count);
        }
        CRC32 crc = new CRC32();
        crc.update(name.getBytes(StandardCharsets.UTF_8));
        return (int) (crc.getValue() % count);
    }

    /**
     * Maps a class file path relative to a test classes directory (e.g. {@code org/foo/BarIT$Inner.class})
     * to the name its shard is chosen by, so nested classes always land with their top-level class.
     */
    public static String topLevelClassName(String relativeClassFilePath) {
        String name = relativeClassFilePath.replace('\\', '/');
        if (name.endsWith(".class")) {
            name = name.substring(0, name.length() - ".class".length());
        }
        int nested = name.indexOf('$', name.lastIndexOf('/') + 1);
        if (nested >= 0) {
            name = name.substring(0, nested);
        }
        return name.replace('/', '.');
    }
}
