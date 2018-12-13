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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JvmErgonomicsTests extends LaunchersTestCase {
    public void testExtractValidHeapSize() {
        assertEquals(Long.valueOf(1024), JvmErgonomics.extractHeapSize(Collections.singletonList("-Xmx1024")));
        assertEquals(Long.valueOf(2L * 1024 * 1024 * 1024), JvmErgonomics.extractHeapSize(Collections.singletonList("-Xmx2g")));
        assertEquals(Long.valueOf(32 * 1024 * 1024), JvmErgonomics.extractHeapSize(Collections.singletonList("-Xmx32M")));
        assertEquals(Long.valueOf(32 * 1024 * 1024), JvmErgonomics.extractHeapSize(Collections.singletonList("-XX:MaxHeapSize=32M")));
    }

    public void testExtractInvalidHeapSize() {
        try {
            JvmErgonomics.extractHeapSize(Collections.singletonList("-Xmx2T"));
            fail("Expected IllegalArgumentException to be raised");
        } catch (IllegalArgumentException expected) {
            assertEquals("Unknown unit [T] for max heap size in [-Xmx2T]", expected.getMessage());
        }
    }

    public void testExtractNoHeapSize() {
        assertNull("No spaces allowed", JvmErgonomics.extractHeapSize(Collections.singletonList("-Xmx 1024")));
        assertNull("JVM option is not present", JvmErgonomics.extractHeapSize(Collections.singletonList("")));
        assertNull("Multiple JVM options per line", JvmErgonomics.extractHeapSize(Collections.singletonList("-Xms2g -Xmx2g")));
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

    public void testLittleMemoryErgonomicChoices() {
        String smallHeap = randomFrom(Arrays.asList("64M", "512M", "1024M", "1G"));
        List<String> expectedChoices = Collections.singletonList("-Dio.netty.allocator.type=unpooled");
        assertEquals(expectedChoices, JvmErgonomics.choose(Arrays.asList("-Xms" + smallHeap, "-Xmx" + smallHeap)));
    }

    public void testPlentyMemoryErgonomicChoices() {
        String largeHeap = randomFrom(Arrays.asList("1025M", "2048M", "2G", "8G"));
        List<String> expectedChoices = Collections.singletonList("-Dio.netty.allocator.type=pooled");
        assertEquals(expectedChoices, JvmErgonomics.choose(Arrays.asList("-Xms" + largeHeap, "-Xmx" + largeHeap)));
    }
}
