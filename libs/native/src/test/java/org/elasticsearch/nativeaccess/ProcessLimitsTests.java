/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ProcessLimitsTests extends ESTestCase {

    private static final NativeAccess nativeAccess = NativeAccess.instance();

    public void testSetMaximumNumberOfThreads() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max processes")) {
                    final String[] fields = line.split("\\s+");
                    final long limit = "unlimited".equals(fields[2]) ? ProcessLimits.UNLIMITED : Long.parseLong(fields[2]);
                    assertThat(nativeAccess.getProcessLimits().maxThreads(), equalTo(limit));
                    return;
                }
            }
            fail("should have read max processes from /proc/self/limits");
        } else {
            assertThat(nativeAccess.getProcessLimits().maxThreads(), equalTo(-1L));
        }
    }

    public void testSetMaxSizeVirtualMemory() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max address space")) {
                    final String[] fields = line.split("\\s+");
                    final long limit = "unlimited".equals(fields[3]) ? ProcessLimits.UNLIMITED : Long.parseLong(fields[3]);
                    assertThat(nativeAccess.getProcessLimits().maxVirtualMemorySize(), equalTo(limit));
                    return;
                }
            }
            fail("should have read max size virtual memory from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(nativeAccess.getProcessLimits().maxVirtualMemorySize(), greaterThanOrEqualTo(0L));
        } else {
            assertThat(nativeAccess.getProcessLimits().maxVirtualMemorySize(), equalTo(ProcessLimits.UNKNOWN));
        }
    }

    public void testSetMaxFileSize() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max file size")) {
                    final String[] fields = line.split("\\s+");
                    final long limit = "unlimited".equals(fields[3]) ? ProcessLimits.UNLIMITED : Long.parseLong(fields[3]);
                    assertThat(nativeAccess.getProcessLimits().maxFileSize(), equalTo(limit));
                    return;
                }
            }
            fail("should have read max file size from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(nativeAccess.getProcessLimits().maxFileSize(), greaterThanOrEqualTo(0L));
        } else {
            assertThat(nativeAccess.getProcessLimits().maxFileSize(), equalTo(ProcessLimits.UNKNOWN));
        }
    }

}
