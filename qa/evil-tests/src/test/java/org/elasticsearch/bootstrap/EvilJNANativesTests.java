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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
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
            if (!lines.isEmpty()) {
                for (String line : lines) {
                    if (line != null && line.startsWith("Max processes")) {
                        final String[] fields = line.split("\\s+");
                        final long limit = Long.parseLong(fields[2]);
                        assertThat(JNANatives.MAX_NUMBER_OF_THREADS, equalTo(limit));
                        return;
                    }
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
            if (!lines.isEmpty()) {
                for (String line : lines) {
                    if (line != null && line.startsWith("Max address space")) {
                        final String[] fields = line.split("\\s+");
                        final String limit = fields[3];
                        assertEquals(JNANatives.rlimitToString(JNANatives.MAX_SIZE_VIRTUAL_MEMORY), limit);
                        return;
                    }
                }
            }
            fail("should have read max size virtual memory from /proc/self/limits");
        } else if (Constants.MAC_OS_X) {
            assertThat(JNANatives.MAX_SIZE_VIRTUAL_MEMORY, anyOf(equalTo(Long.MIN_VALUE), greaterThanOrEqualTo(0L)));
        } else {
            assertThat(JNANatives.MAX_SIZE_VIRTUAL_MEMORY, equalTo(Long.MIN_VALUE));
        }
    }

}
