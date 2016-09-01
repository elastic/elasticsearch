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

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class OsInfoTests extends ESTestCase {

    public void testSerialization() throws IOException {
        int availableProcessors = randomIntBetween(1, 64);
        int allocatedProcessors = randomIntBetween(1, availableProcessors);
        long refreshInterval;
        if (randomBoolean()) {
            refreshInterval = -1;
        } else {
            refreshInterval = randomPositiveLong();
        }
        String name = randomAsciiOfLengthBetween(3, 10);
        String arch = randomAsciiOfLengthBetween(3, 10);
        String version = randomAsciiOfLengthBetween(3, 10);
        OsInfo osInfo = new OsInfo(refreshInterval, availableProcessors, allocatedProcessors, name, arch, version);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            osInfo.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                OsInfo deserializedOsInfo = new OsInfo(in);
                assertEquals(osInfo.getRefreshInterval(), deserializedOsInfo.getRefreshInterval());
                assertEquals(osInfo.getAvailableProcessors(), deserializedOsInfo.getAvailableProcessors());
                assertEquals(osInfo.getAllocatedProcessors(), deserializedOsInfo.getAllocatedProcessors());
                assertEquals(osInfo.getName(), deserializedOsInfo.getName());
                assertEquals(osInfo.getArch(), deserializedOsInfo.getArch());
                assertEquals(osInfo.getVersion(), deserializedOsInfo.getVersion());
            }
        }
    }
}
