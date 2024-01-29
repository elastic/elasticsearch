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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EvilNativesTests extends ESTestCase {

    public void testSetMaximumNumberOfThreads() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max processes")) {
                    final String[] fields = line.split("\\s+");
                    final long limit = "unlimited".equals(fields[2])
                        ? NativeAccess.instance().getRlimitInfinity()
                        : Long.parseLong(fields[2]);
                    assertThat(NativeAccess.instance().getMaxNumberOfThreads(), equalTo(limit));
                    return;
                }
            }
            fail("should have read max processes from /proc/self/limits");
        } else {
            assertThat(NativeAccess.instance().getMaxNumberOfThreads(), equalTo(-1L));
        }
    }

    public void testSetMaxSizeVirtualMemory() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max address space")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(JNANatives.rlimitToString(NativeAccess.instance().getMaxVirtualMemorySize()), equalTo(limit));
                    return;
                }
            }
            fail("should have read max size virtual memory from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(NativeAccess.instance().getMaxVirtualMemorySize(), anyOf(equalTo(Long.MIN_VALUE), greaterThanOrEqualTo(0L)));
        } else {
            assertThat(NativeAccess.instance().getMaxVirtualMemorySize(), equalTo(Long.MIN_VALUE));
        }
    }

    public void testSetMaxFileSize() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max file size")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(JNANatives.rlimitToString(NativeAccess.instance().getMaxFileSize()), equalTo(limit));
                    return;
                }
            }
            fail("should have read max file size from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(NativeAccess.instance().getMaxFileSize(), anyOf(equalTo(Long.MIN_VALUE), greaterThanOrEqualTo(0L)));
        } else {
            assertThat(NativeAccess.instance().getMaxFileSize(), equalTo(Long.MIN_VALUE));
        }
    }

}
