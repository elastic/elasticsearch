/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.Base64;

/**
 * A class that extends {@link TimeBasedUUIDGenerator} re-arranging document _id bytes in such
 * a way to take advantage of shared prefixes and favor compression of the term dictionary.
 * The idea is to generate the id such that never-changing or slowly changing parts of the id
 * come first so that large sequences of document ids share the same prefix. This should result
 * in a more compact term dictionary.
 */
public class TimeBasedKOrderedUUIDGenerator extends TimeBasedUUIDGenerator {
    private static final Base64.Encoder BASE_64_NO_PADDING = Base64.getEncoder().withoutPadding();

    @Override
    public String getBase64UUID() {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0x00FF_FFFF;

        long timestamp = this.lastTimestamp.accumulateAndGet(
            currentTimeMillis(),
            sequenceId == 0 ? (lastTimestamp, currentTimeMillis) -> Math.max(lastTimestamp, currentTimeMillis) + 1 : Math::max
        );

        final byte[] uuidBytes = new byte[15];
        int i = 0;

        uuidBytes[i++] = (byte) (timestamp >>> 40); // changes every 35 years
        uuidBytes[i++] = (byte) (timestamp >>> 32); // changes every ~50 days
        uuidBytes[i++] = (byte) (timestamp >>> 24); // changes every ~4.5h
        uuidBytes[i++] = (byte) (timestamp >>> 16); // changes every ~65 secs

        // MAC address of the coordinator might change if there are many coordinators in the cluster
        // and the indexing api does not necessarily target the same coordinator.
        byte[] macAddress = macAddress();
        assert macAddress.length == 6;
        System.arraycopy(macAddress, 0, uuidBytes, i, macAddress.length);
        i += macAddress.length;
        uuidBytes[i++] = (byte) (sequenceId >>> 16);

        // From hereinafter everything is almost like random and does not compress well
        // due to unlikely prefix-sharing
        uuidBytes[i++] = (byte) (timestamp >>> 8);
        uuidBytes[i++] = (byte) (sequenceId >>> 8);
        uuidBytes[i++] = (byte) timestamp;
        uuidBytes[i++] = (byte) sequenceId;
        assert i == uuidBytes.length;

        return BASE_64_NO_PADDING.encodeToString(uuidBytes);
    }
}
