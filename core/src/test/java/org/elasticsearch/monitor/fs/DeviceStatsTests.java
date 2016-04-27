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

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;

public class DeviceStatsTests extends ESTestCase {

    public void testDeviceStats() {
        final int majorDeviceNumber = randomIntBetween(1, 1 << 8);
        final int minorDeviceNumber = randomIntBetween(0, 1 << 5);
        final String deviceName = randomAsciiOfLength(3);
        final int readsCompleted = randomIntBetween(1, 1 << 16);
        final int sectorsRead = randomIntBetween(8 * readsCompleted, 16 * readsCompleted);
        final int readMilliseconds = randomIntBetween(readsCompleted, 2 * readsCompleted);
        final int writesCompleted = randomIntBetween(1, 1 << 16);
        final int sectorsWritten = randomIntBetween(8 * writesCompleted, 16 * writesCompleted);
        final int writeMilliseconds = randomIntBetween(writesCompleted, 2 * writesCompleted);
        final int weightedMillseconds =
                randomIntBetween(readMilliseconds, 2 * readMilliseconds) + randomIntBetween(writeMilliseconds, 2 * writeMilliseconds);
        final long relativeTime = randomIntBetween(0, Integer.MAX_VALUE);
        final long elapsed = randomIntBetween((int) TimeUnit.SECONDS.toNanos(1), (int) TimeUnit.SECONDS.toNanos(2));

        FsInfo.DeviceStats previous = new FsInfo.DeviceStats(
                majorDeviceNumber,
                minorDeviceNumber,
                deviceName,
                readsCompleted,
                sectorsRead,
                readMilliseconds,
                writesCompleted,
                sectorsWritten,
                writeMilliseconds,
                weightedMillseconds,
                relativeTime,
                null);
        FsInfo.DeviceStats current = new FsInfo.DeviceStats(
                majorDeviceNumber,
                minorDeviceNumber,
                deviceName,
                readsCompleted + 1024,
                sectorsRead + 16384,
                readMilliseconds + 2048,
                writesCompleted + 2048,
                sectorsWritten + 32768,
                writeMilliseconds + 4096,
                weightedMillseconds + 4096 + 8192,
                relativeTime + elapsed,
                previous);
        final long scale = TimeUnit.SECONDS.toNanos(1);
        assertThat(current.iops(), equalTo(scale * 3072f / elapsed));
        assertThat(current.readsPerSecond(), equalTo(scale * 1024f / elapsed));
        assertThat(current.writesPerSecond(), equalTo(scale * 2048f / elapsed));
        assertThat(current.readKilobytesPerSecond(), equalTo(scale * 8192f / elapsed));
        assertThat(current.writeKilobytesPerSecond(), equalTo(scale * 16384f / elapsed));
        assertThat(current.averageRequestSizeInKilobytes(), equalTo(8f));
        assertThat(current.averageResidentRequests(), equalTo(scale * 12288f / elapsed / 1000));
        assertThat(current.averageAwaitTimeInMilliseconds(), equalTo(2f));
        assertThat(current.averageReadAwaitTimeInMilliseconds(), equalTo(2f));
        assertThat(current.averageWriteAwaitTimeInMilliseconds(), equalTo(2f));
    }

}