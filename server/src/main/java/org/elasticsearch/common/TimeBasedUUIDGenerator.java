/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * These are essentially flake ids but we use 6 (not 8) bytes for timestamp, and use 3 (not 2) bytes for sequence number. We also reorder
 * bytes in a way that does not make ids sort in order anymore, but is more friendly to the way that the Lucene terms dictionary is
 * structured.
 * For more information about flake ids, check out
 * https://archive.fo/2015.07.08-082503/http://www.boundary.com/blog/2012/01/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang/
 */

class TimeBasedUUIDGenerator implements UUIDGenerator {

    // We only use bottom 3 bytes for the sequence number. Paranoia: init with random int so that if JVM/OS/machine goes down, clock slips
    // backwards, and JVM comes back up, we are less likely to be on the same sequenceNumber at the same time:
    private final AtomicInteger sequenceNumber = new AtomicInteger(SecureRandomHolder.INSTANCE.nextInt());

    // Used to ensure clock moves forward:
    private final AtomicLong lastTimestamp = new AtomicLong(0);

    private static final byte[] SECURE_MUNGED_ADDRESS = MacAddressProvider.getSecureMungedAddress();

    static {
        assert SECURE_MUNGED_ADDRESS.length == 6;
    }

    // protected for testing
    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // protected for testing
    protected byte[] macAddress() {
        return SECURE_MUNGED_ADDRESS;
    }

    @Override
    public String getBase64UUID() {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0xffffff;
        long currentTimeMillis = currentTimeMillis();

        long timestamp = this.lastTimestamp.updateAndGet(lastTimestamp -> {
            // Don't let timestamp go backwards, at least "on our watch" (while this JVM is running). We are
            // still vulnerable if we are shut down, clock goes backwards, and we restart... for this we
            // randomize the sequenceNumber on init to decrease chance of collision:
            long nonBackwardsTimestamp = Math.max(lastTimestamp, currentTimeMillis);

            if (sequenceId == 0) {
                // Always force the clock to increment whenever sequence number is 0, in case we have a long
                // time-slip backwards:
                nonBackwardsTimestamp++;
            }

            return nonBackwardsTimestamp;
        });

        final byte[] uuidBytes = new byte[15];
        int i = 0;

        // We have auto-generated ids, which are usually used for append-only workloads.
        // So we try to optimize the order of bytes for indexing speed (by having quite
        // unique bytes close to the beginning of the ids so that sorting is fast) and
        // compression (by making sure we share common prefixes between enough ids),
        // but not necessarily for lookup speed (by having the leading bytes identify
        // segments whenever possible)

        // Blocks in the block tree have between 25 and 48 terms. So all prefixes that
        // are shared by ~30 terms should be well compressed. I first tried putting the
        // two lower bytes of the sequence id in the beginning of the id, but compression
        // is only triggered when you have at least 30*2^16 ~= 2M documents in a segment,
        // which is already quite large. So instead, we are putting the 1st and 3rd byte
        // of the sequence number so that compression starts to be triggered with smaller
        // segment sizes and still gives pretty good indexing speed. We use the sequenceId
        // rather than the timestamp because the distribution of the timestamp depends too
        // much on the indexing rate, so it is less reliable.

        uuidBytes[i++] = (byte) sequenceId;
        // changes every 65k docs, so potentially every second if you have a steady indexing rate
        uuidBytes[i++] = (byte) (sequenceId >>> 16);

        // Now we start focusing on compression and put bytes that should not change too often.
        uuidBytes[i++] = (byte) (timestamp >>> 16); // changes every ~65 secs
        uuidBytes[i++] = (byte) (timestamp >>> 24); // changes every ~4.5h
        uuidBytes[i++] = (byte) (timestamp >>> 32); // changes every ~50 days
        uuidBytes[i++] = (byte) (timestamp >>> 40); // changes every 35 years
        byte[] macAddress = macAddress();
        assert macAddress.length == 6;
        System.arraycopy(macAddress, 0, uuidBytes, i, macAddress.length);
        i += macAddress.length;

        // Finally we put the remaining bytes, which will likely not be compressed at all.
        uuidBytes[i++] = (byte) (timestamp >>> 8);
        uuidBytes[i++] = (byte) (sequenceId >>> 8);
        uuidBytes[i++] = (byte) timestamp;

        assert i == uuidBytes.length;

        return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes);
    }
}
