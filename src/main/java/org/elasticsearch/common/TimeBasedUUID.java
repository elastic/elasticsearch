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
import java.nio.ByteBuffer;

public class TimeBasedUUID implements UUIDGenerator{

    private PaddedAtomicLong sequenceNumber = new PaddedAtomicLong();
    private PaddedAtomicLong lastTimestamp = new PaddedAtomicLong();

    byte[] secureMungedAddress = MacAddressProvider.getSecureMungedAddress();

    @Override
    public String getBase64UUID()  {
        long timestamp = System.currentTimeMillis();
        long last;

        final byte[] uuidBytes = new byte[22];
        do {
            last = lastTimestamp.get();
            if (lastTimestamp.compareAndSet(last, timestamp)) {
                break;
            }
        } while(last < timestamp);

        ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        buffer.putLong(timestamp);
        buffer.put(secureMungedAddress);
        buffer.putLong(sequenceNumber.incrementAndGet());

        try {
            byte[] encoded = Base64.encodeBytesToBytes(uuidBytes, 0, uuidBytes.length, Base64.URL_SAFE);
            // we know the bytes are 22, and not a multi of 3, so remove the 2 padding chars that are added
            assert encoded[encoded.length - 1] == '=';
            assert encoded[encoded.length - 2] == '=';
            // we always have padding of two at the end, encode it differently
            return new String(encoded, 0, encoded.length - 2, Base64.PREFERRED_ENCODING);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("should not be thrown");
        }
    }

    public static long getTimestampFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"==",Base64.URL_SAFE);
        ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        return buffer.getLong();
    }

    public static long getSequenceNumberFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"==",Base64.URL_SAFE);
        ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        buffer.getLong();
        byte[] mungedAddress = new byte[6];
        buffer.get(mungedAddress);
        return buffer.getLong();
    }

    public static byte[] getAddressBytesFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"==",Base64.URL_SAFE);
        ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        buffer.getLong();
        byte[] mungedAddress = new byte[6];
        buffer.get(mungedAddress);
        return mungedAddress;
    }

}