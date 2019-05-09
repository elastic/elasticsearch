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

package org.elasticsearch.tools.launchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JvmErgonomicsTests extends LaunchersTestCase {

    public void testExtractValidHeapSizeUsingXmx() throws InterruptedException, IOException {
        assertThat(
                JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx2g"))),
                equalTo(2L << 30));
    }

    public void testExtractValidHeapSizeUsingMaxHeapSize() throws InterruptedException, IOException {
        assertThat(
                JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-XX:MaxHeapSize=2g"))),
                equalTo(2L << 30));
    }

    public void testExtractValidHeapSizeNoOptionPresent() throws InterruptedException, IOException {
        assertThat(
                JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.emptyList())),
                greaterThan(0L));
    }

    public void testHeapSizeInvalid() throws InterruptedException, IOException {
        try {
            JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx2Z")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Invalid maximum heap size: -Xmx2Z"))));
        }
    }

    public void testHeapSizeTooSmall() throws InterruptedException, IOException {
        try {
            JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx1024")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Too small maximum heap"))));
        }
    }

    public void testHeapSizeWithSpace() throws InterruptedException, IOException {
        try {
            JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx 1024")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Invalid maximum heap size: -Xmx 1024"))));
        }
    }

    public void testExtractSystemProperties() {
        Map<String, String> expectedSystemProperties = new HashMap<>();
        expectedSystemProperties.put("file.encoding", "UTF-8");
        expectedSystemProperties.put("kv.setting", "ABC=DEF");

        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(
            Arrays.asList("-Dfile.encoding=UTF-8", "-Dkv.setting=ABC=DEF"));

        assertEquals(expectedSystemProperties, parsedSystemProperties);
    }

    public void testExtractNoSystemProperties() {
        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(Arrays.asList("-Xms1024M", "-Xmx1024M"));
        assertTrue(parsedSystemProperties.isEmpty());
    }

    public void testLittleMemoryErgonomicChoices() throws InterruptedException, IOException {
        String smallHeap = randomFrom(Arrays.asList("64M", "512M", "1024M", "1G"));
        List<String> expectedChoices = Collections.singletonList("-Dio.netty.allocator.type=unpooled");
        assertEquals(expectedChoices, JvmErgonomics.choose(Arrays.asList("-Xms" + smallHeap, "-Xmx" + smallHeap)));
    }

    public void testPlentyMemoryErgonomicChoices() throws InterruptedException, IOException {
        String largeHeap = randomFrom(Arrays.asList("1025M", "2048M", "2G", "8G"));
        List<String> expectedChoices = Collections.singletonList("-Dio.netty.allocator.type=pooled");
        assertEquals(expectedChoices, JvmErgonomics.choose(Arrays.asList("-Xms" + largeHeap, "-Xmx" + largeHeap)));
    }

}
