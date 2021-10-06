/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EvilJNANativesTests extends ESTestCase {

    public void testSetMaximumNumberOfThreads() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max processes")) {
                    final String[] fields = line.split("\\s+");
                    final long limit =
                            "unlimited".equals(fields[2])
                                    ? JNACLibrary.RLIM_INFINITY
                                    : Long.parseLong(fields[2]);
                    assertThat(JNANatives.MAX_NUMBER_OF_THREADS, equalTo(limit));
                    return;
                }
            }
            fail("should have read max processes from /proc/self/limits");
        } else {
            assertThat(JNANatives.MAX_NUMBER_OF_THREADS, equalTo(-1L));
        }
    }

    public void testSetMaxSizeVirtualMemory() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max address space")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(
                            JNANatives.rlimitToString(JNANatives.MAX_SIZE_VIRTUAL_MEMORY),
                            equalTo(limit));
                    return;
                }
            }
            fail("should have read max size virtual memory from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(
                    JNANatives.MAX_SIZE_VIRTUAL_MEMORY,
                    anyOf(equalTo(Long.MIN_VALUE), greaterThanOrEqualTo(0L)));
        } else {
            assertThat(JNANatives.MAX_SIZE_VIRTUAL_MEMORY, equalTo(Long.MIN_VALUE));
        }
    }

    public void testSetMaxFileSize() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max file size")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(
                            JNANatives.rlimitToString(JNANatives.MAX_FILE_SIZE),
                            equalTo(limit));
                    return;
                }
            }
            fail("should have read max file size from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(
                    JNANatives.MAX_FILE_SIZE,
                    anyOf(equalTo(Long.MIN_VALUE), greaterThanOrEqualTo(0L)));
        } else {
            assertThat(JNANatives.MAX_FILE_SIZE, equalTo(Long.MIN_VALUE));
        }
    }

}
