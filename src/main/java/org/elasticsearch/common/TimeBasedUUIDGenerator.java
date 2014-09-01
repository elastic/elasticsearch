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

package org.elasticsearch.common;

import org.elasticsearch.ElasticsearchIllegalStateException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** These are essentially flake ids (http://boundary.com/blog/2012/01/12/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang) but
 *  we use 6 (not 8) bytes for timestamp, and use 3 (not 2) bytes for sequence number. */

class TimeBasedUUIDGenerator implements UUIDGenerator {

    // We only use bottom 3 bytes for the sequence number.  Paranoia: init with random int so that if JVM/OS/machine goes down, clock slips
    // backwards, and JVM comes back up, we are less likely to be on the same sequenceNumber at the same time:
    private final AtomicInteger sequenceNumber = new AtomicInteger(SecureRandomHolder.INSTANCE.nextInt());

    // Used to ensure clock moves forward:
    private final AtomicLong lastTimestamp = new AtomicLong();

    private static final byte[] secureMungedAddress = MacAddressProvider.getSecureMungedAddress();

    static {
        assert secureMungedAddress.length == 6;
    }

    /** Puts the lower numberOfLongBytes from l into the array, starting index pos. */
    private static void putLong(byte[] array, long l, int pos, int numberOfLongBytes) {
        for (int i=0; i<numberOfLongBytes; ++i) {
            array[pos+numberOfLongBytes-i-1] = (byte)( l >> (i*8) & 0xff);
        }
    }

    @Override
    public String getBase64UUID()  {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0xffffff;

        // Deal with clock moving backwards "on our watch" by recording the last timestamp only if it increased:
        long timestamp;
        while (true) {
            final long last = lastTimestamp.get();
            timestamp = System.currentTimeMillis();
            if ((sequenceId & 0xffff) == 0) {
                // If we have a long time-slip backwards, force the clock to increment whenever lower 4 bytes of sequence number wraps
                // around to 0.  This is only best-effort (not perfectly thread safe):
                timestamp++;
            }
            if (timestamp <= last) {
                // Clock slipped backwards:
                timestamp = last;
                break;
            } else if (lastTimestamp.compareAndSet(last, timestamp)) {
                break;
            }
        }

        final byte[] uuidBytes = new byte[15];

        // Only use lower 6 bytes of the timestamp (this will suffice beyond the year 10000):
        putLong(uuidBytes, timestamp, 0, 6);

        // MAC address adds 6 bytes:
        System.arraycopy(secureMungedAddress, 0, uuidBytes, 6, secureMungedAddress.length);

        // Sequence number adds 3 bytes:
        putLong(uuidBytes, sequenceId, 12, 3);

        assert 9 + secureMungedAddress.length == uuidBytes.length;

        byte[] encoded;
        try {
            encoded = Base64.encodeBytesToBytes(uuidBytes, 0, uuidBytes.length, Base64.URL_SAFE);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("should not be thrown", e);
        }

        // We are a multiple of 3 bytes so we should not see any padding:
        assert encoded[encoded.length - 1] != '=';
        return new String(encoded, 0, encoded.length, Base64.PREFERRED_ENCODING);
    }
}
