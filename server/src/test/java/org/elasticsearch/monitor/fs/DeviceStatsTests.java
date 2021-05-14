/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final int ioTime = randomIntBetween(1, 1 << 16);

        FsInfo.DeviceStats previous = new FsInfo.DeviceStats(
            majorDeviceNumber,
            minorDeviceNumber,
            deviceName,
            readsCompleted,
            sectorsRead,
            writesCompleted,
            sectorsWritten,
            ioTime,
            null);
        FsInfo.DeviceStats current = new FsInfo.DeviceStats(
            majorDeviceNumber,
            minorDeviceNumber,
            deviceName,
            readsCompleted + 1024,
            sectorsRead + 16384,
            writesCompleted + 2048,
            sectorsWritten + 32768,
            ioTime + 128,
            previous);
        assertThat(current.operations(), equalTo(1024L + 2048L));
        assertThat(current.readOperations(), equalTo(1024L));
        assertThat(current.writeOperations(), equalTo(2048L));
        assertThat(current.readKilobytes(), equalTo(16384L / 2));
        assertThat(current.writeKilobytes(), equalTo(32768L / 2));
        assertThat(current.ioTimeInMillis(), equalTo(128L));
    }

}
