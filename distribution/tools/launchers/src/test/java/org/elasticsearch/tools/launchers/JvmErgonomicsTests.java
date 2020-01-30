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

import org.elasticsearch.tools.java_version_checker.JavaVersion;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JvmErgonomicsTests extends LaunchersTestCase {

    public void testExtractValidHeapSizeUsingXmx() throws InterruptedException, IOException {
        assertThat(JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx2g"))), equalTo(2L << 30));
    }

    public void testExtractValidHeapSizeUsingMaxHeapSize() throws InterruptedException, IOException {
        assertThat(
            JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-XX:MaxHeapSize=2g"))),
            equalTo(2L << 30)
        );
    }

    public void testExtractValidHeapSizeNoOptionPresent() throws InterruptedException, IOException {
        // Muted for jdk8/Windows, see: https://github.com/elastic/elasticsearch/issues/47384
        assumeFalse(System.getProperty("os.name").startsWith("Windows") && JavaVersion.majorVersion(JavaVersion.CURRENT) == 8);
        assertThat(JvmErgonomics.extractHeapSize(JvmErgonomics.finalJvmOptions(Collections.emptyList())), greaterThan(0L));
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
            assertThat(
                e,
                anyOf(hasToString(containsString("Too small initial heap")), hasToString(containsString("Too small maximum heap")))
            );
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

    public void testMaxDirectMemorySizeUnset() throws InterruptedException, IOException {
        assertThat(
            JvmErgonomics.extractMaxDirectMemorySize(JvmErgonomics.finalJvmOptions(Collections.singletonList("-Xmx1g"))),
            equalTo(0L)
        );
    }

    public void testMaxDirectMemorySizeSet() throws InterruptedException, IOException {
        assertThat(
            JvmErgonomics.extractMaxDirectMemorySize(
                JvmErgonomics.finalJvmOptions(Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=512m"))
            ),
            equalTo(512L << 20)
        );
    }

    public void testExtractSystemProperties() {
        Map<String, String> expectedSystemProperties = new HashMap<>();
        expectedSystemProperties.put("file.encoding", "UTF-8");
        expectedSystemProperties.put("kv.setting", "ABC=DEF");

        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(
            Arrays.asList("-Dfile.encoding=UTF-8", "-Dkv.setting=ABC=DEF")
        );

        assertEquals(expectedSystemProperties, parsedSystemProperties);
    }

    public void testExtractNoSystemProperties() {
        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(Arrays.asList("-Xms1024M", "-Xmx1024M"));
        assertTrue(parsedSystemProperties.isEmpty());
    }

    public void testMaxDirectMemorySizeChoice() throws InterruptedException, IOException {
        // Muted for jdk8/Windows, see: https://github.com/elastic/elasticsearch/issues/47384
        assumeFalse(System.getProperty("os.name").startsWith("Windows") && JavaVersion.majorVersion(JavaVersion.CURRENT) == 8);
        final Map<String, String> heapMaxDirectMemorySize = new HashMap<>();
        heapMaxDirectMemorySize.put("64M", Long.toString((64L << 20) / 2));
        heapMaxDirectMemorySize.put("512M", Long.toString((512L << 20) / 2));
        heapMaxDirectMemorySize.put("1024M", Long.toString((1024L << 20) / 2));
        heapMaxDirectMemorySize.put("1G", Long.toString((1L << 30) / 2));
        heapMaxDirectMemorySize.put("2048M", Long.toString((2048L << 20) / 2));
        heapMaxDirectMemorySize.put("2G", Long.toString((2L << 30) / 2));
        heapMaxDirectMemorySize.put("8G", Long.toString((8L << 30) / 2));
        final String heapSize = randomFrom(heapMaxDirectMemorySize.keySet().toArray(new String[0]));
        assertThat(
            JvmErgonomics.choose(Arrays.asList("-Xms" + heapSize, "-Xmx" + heapSize)),
            hasItem("-XX:MaxDirectMemorySize=" + heapMaxDirectMemorySize.get(heapSize))
        );
    }

    public void testMaxDirectMemorySizeChoiceWhenSet() throws InterruptedException, IOException {
        List<String> derivedSettingList = JvmErgonomics.choose(Arrays.asList("-Xms5g", "-Xmx5g", "-XX:MaxDirectMemorySize=4g"));
        assertThat(
            derivedSettingList,
            // if MaxDirectMemorySize is set, we shouldn't derive our own value for it
            everyItem(not(startsWith("-XX:MaxDirectMemorySize=")))
        );
    }

}
