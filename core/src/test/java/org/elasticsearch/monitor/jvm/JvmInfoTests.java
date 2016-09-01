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

package org.elasticsearch.monitor.jvm;

import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class JvmInfoTests extends ESTestCase {

    public void testUseG1GC() {
        // if we are running on HotSpot, and the test JVM was started
        // with UseG1GC, then JvmInfo should successfully report that
        // G1GC is enabled
        if (Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK")) {
            assertEquals(Boolean.toString(isG1GCEnabled()), JvmInfo.jvmInfo().useG1GC());
        } else {
            assertEquals("unknown", JvmInfo.jvmInfo().useG1GC());
        }
    }

    private boolean isG1GCEnabled() {
        final String argline = System.getProperty("tests.jvm.argline");
        final boolean g1GCEnabled = flagIsEnabled(argline, "UseG1GC");
        // for JDK 9 the default collector when no collector is specified is G1 GC
        final boolean versionIsAtLeastJava9 = JavaVersion.current().compareTo(JavaVersion.parse("9")) >= 0;
        final boolean noOtherCollectorSpecified =
                argline == null ||
                        (!flagIsEnabled(argline, "UseParNewGC") &&
                                !flagIsEnabled(argline, "UseParallelGC") &&
                                !flagIsEnabled(argline, "UseParallelOldGC") &&
                                !flagIsEnabled(argline, "UseSerialGC") &&
                                !flagIsEnabled(argline, "UseConcMarkSweepGC"));
        return g1GCEnabled || (versionIsAtLeastJava9 && noOtherCollectorSpecified);
    }

    private boolean flagIsEnabled(String argline, String flag) {
        final boolean containsPositiveFlag = argline != null && argline.contains("-XX:+" + flag);
        if (!containsPositiveFlag) return false;
        final int index = argline.lastIndexOf(flag);
        return argline.charAt(index - 1) == '+';
    }

    public void testSerialization() throws IOException {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            jvmInfo.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                JvmInfo deserializedJvmInfo = new JvmInfo(in);
                assertEquals(jvmInfo.getVersion(), deserializedJvmInfo.getVersion());
                assertEquals(jvmInfo.getVmName(), deserializedJvmInfo.getVmName());
                assertEquals(jvmInfo.getVmVendor(), deserializedJvmInfo.getVmVendor());
                assertEquals(jvmInfo.getVmVersion(), deserializedJvmInfo.getVmVersion());
                assertEquals(jvmInfo.getBootClassPath(), deserializedJvmInfo.getBootClassPath());
                assertEquals(jvmInfo.getClassPath(), deserializedJvmInfo.getClassPath());
                assertEquals(jvmInfo.getPid(), deserializedJvmInfo.getPid());
                assertEquals(jvmInfo.getStartTime(), deserializedJvmInfo.getStartTime());
                assertEquals(jvmInfo.versionUpdatePack(), deserializedJvmInfo.versionUpdatePack());
                assertEquals(jvmInfo.versionAsInteger(), deserializedJvmInfo.versionAsInteger());
                assertEquals(jvmInfo.getMem().getDirectMemoryMax(), deserializedJvmInfo.getMem().getDirectMemoryMax());
                assertEquals(jvmInfo.getMem().getHeapInit(), deserializedJvmInfo.getMem().getHeapInit());
                assertEquals(jvmInfo.getMem().getHeapMax(), deserializedJvmInfo.getMem().getHeapMax());
                assertEquals(jvmInfo.getMem().getNonHeapInit(), deserializedJvmInfo.getMem().getNonHeapInit());
                assertEquals(jvmInfo.getMem().getNonHeapMax(), deserializedJvmInfo.getMem().getNonHeapMax());
                assertEquals(jvmInfo.getSystemProperties(), deserializedJvmInfo.getSystemProperties());
                assertArrayEquals(jvmInfo.getInputArguments(), deserializedJvmInfo.getInputArguments());
                assertArrayEquals(jvmInfo.getGcCollectors(), deserializedJvmInfo.getGcCollectors());
                assertArrayEquals(jvmInfo.getMemoryPools(), deserializedJvmInfo.getMemoryPools());
                assertEquals(jvmInfo.useCompressedOops(), deserializedJvmInfo.useCompressedOops());
                assertEquals(-1, deserializedJvmInfo.getConfiguredInitialHeapSize());
                assertEquals(-1, deserializedJvmInfo.getConfiguredMaxHeapSize());
                assertEquals("unknown", deserializedJvmInfo.useG1GC());
                assertNull(deserializedJvmInfo.onError());
                assertNull(deserializedJvmInfo.onOutOfMemoryError());
            }
        }
    }
}
