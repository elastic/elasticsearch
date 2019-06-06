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

package org.elasticsearch.monitor.fs;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class DeviceStatsTests extends ESTestCase {

    public void testDeviceStats() {
        final int majorDeviceNumber = randomIntBetween(1, 1 << 8);
        final int minorDeviceNumber = randomIntBetween(0, 1 << 5);
        final String deviceName = randomAlphaOfLength(3);
        final int readsCompleted = randomIntBetween(1, 1 << 16);
        final int sectorsRead = randomIntBetween(8 * readsCompleted, 16 * readsCompleted);
        final int writesCompleted = randomIntBetween(1, 1 << 16);
        final int sectorsWritten = randomIntBetween(8 * writesCompleted, 16 * writesCompleted);

        FsInfo.DeviceStats previous = new FsInfo.DeviceStats(
            majorDeviceNumber,
            minorDeviceNumber,
            deviceName,
            readsCompleted,
            sectorsRead,
            writesCompleted,
            sectorsWritten,
            null);
        FsInfo.DeviceStats current = new FsInfo.DeviceStats(
            majorDeviceNumber,
            minorDeviceNumber,
            deviceName,
            readsCompleted + 1024,
            sectorsRead + 16384,
            writesCompleted + 2048,
            sectorsWritten + 32768,
            previous);
        assertThat(current.operations(), equalTo(1024L + 2048L));
        assertThat(current.readOperations(), equalTo(1024L));
        assertThat(current.writeOperations(), equalTo(2048L));
        assertThat(current.readKilobytes(), equalTo(16384L / 2));
        assertThat(current.writeKilobytes(), equalTo(32768L / 2));
    }

}
