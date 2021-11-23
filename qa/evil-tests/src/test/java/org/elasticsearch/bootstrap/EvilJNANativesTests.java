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

import static org.elasticsearch.test.hamcrest.OptionalLongMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalLongMatchers.isPresent;
import static org.elasticsearch.test.hamcrest.OptionalLongMatchers.optionalWithValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EvilJNANativesTests extends ESTestCase {

    public void testRetrieveMaximumNumberOfThreads() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max processes")) {
                    final String[] fields = line.split("\\s+");
                    final long limit = "unlimited".equals(fields[2]) ? CLibrary.RLIM_INFINITY : Long.parseLong(fields[2]);
                    assertThat(Natives.maxNumberOfThreads(), optionalWithValue(equalTo(limit)));
                    return;
                }
            }
            fail("should have read max processes from /proc/self/limits");
        } else {
            assertThat(Natives.maxNumberOfThreads(), isEmpty());
        }
    }

    public void testRetrieveMaxSizeVirtualMemory() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max address space")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(Natives.maxNumberOfThreads(), isPresent());
                    assertThat(NativeOperationsImpl.rlimitToString(Natives.maxVirtualMemorySize().getAsLong()), equalTo(limit));
                    return;
                }
            }
            fail("should have read max size virtual memory from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(Natives.maxVirtualMemorySize(), anyOf(isEmpty(), optionalWithValue(greaterThanOrEqualTo(0L))));
        } else {
            assertThat(Natives.maxVirtualMemorySize(), isEmpty());
        }
    }

    public void testSetMaxFileSize() throws IOException {
        if (Constants.LINUX) {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/limits"));
            for (final String line : lines) {
                if (line != null && line.startsWith("Max file size")) {
                    final String[] fields = line.split("\\s+");
                    final String limit = fields[3];
                    assertThat(Natives.maxFileSize(), isPresent());
                    assertThat(NativeOperationsImpl.rlimitToString(Natives.maxFileSize().getAsLong()), equalTo(limit));
                    return;
                }
            }
            fail("should have read max file size from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(Natives.maxFileSize(), anyOf(isEmpty(), optionalWithValue(greaterThanOrEqualTo(0L))));
        } else {
            assertThat(Natives.maxVirtualMemorySize(), isEmpty());
        }
    }

}
