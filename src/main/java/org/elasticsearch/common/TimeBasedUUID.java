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

    private static void putLong(byte[] array, long l, int pos, int numberOfLongBytes){
        for (int i=0; i<numberOfLongBytes; ++i){
            array[pos+numberOfLongBytes-i-1] = (byte)( l >> (i*8) & 0xff);
        }
    }

    private static long getLong(byte[] array, int pos, int numberOfLongBytes){
        byte[] longBytes = new byte[8];
        for (int i = 0; i < numberOfLongBytes && i < 8; ++i){
            longBytes[i+(8-numberOfLongBytes)] = array[i+pos];
        }
        return ByteBuffer.wrap(longBytes).getLong();
    }

    @Override
    public String getBase64UUID()  {
        long timestamp;
        long last;
        final byte[] uuidBytes = new byte[20];
        do {
            last = lastTimestamp.get();
            timestamp = System.currentTimeMillis();
            if (last > timestamp){
                timestamp = last; //If we inc here we risk running away but we
                                  // have to be sure that we won't use all the sequence number space in a single slip
            }
            if (lastTimestamp.compareAndSet(last, timestamp)) { //Still do the set to make sure we haven't recovered
                                                                //from the slip in a different thread
                break;
            }
        } while(last < timestamp);

        putLong(uuidBytes,timestamp,0,6); //Only use 6 bytes of the timestamp this will suffice beyond the year 10000
        System.arraycopy(secureMungedAddress,0,uuidBytes,6,secureMungedAddress.length);
        putLong(uuidBytes,sequenceNumber.incrementAndGet(),12,8);

        try {
            byte[] encoded = Base64.encodeBytesToBytes(uuidBytes, 0, uuidBytes.length, Base64.URL_SAFE);
            // we know the bytes are 20, which will add a padding to 21 so we can remove it
            assert encoded[encoded.length - 1] == '=';
            // we always have padding of one at the end, encode it differently
            return new String(encoded, 0, encoded.length - 1, Base64.PREFERRED_ENCODING);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("should not be thrown");
        }
    }

    public static long getTimestampFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"=",Base64.URL_SAFE);
        return getLong(uuidBytes,0,6);
    }

    public static long getSequenceNumberFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"=",Base64.URL_SAFE);
        return getLong(uuidBytes,12,8);
    }

    public static byte[] getAddressBytesFromUUID(String uuid) throws IOException {
        byte[] uuidBytes = Base64.decode(uuid+"=",Base64.URL_SAFE);
        byte[] mungedAddress = new byte[6];
        System.arraycopy(uuidBytes,6,mungedAddress,0,6);
        return mungedAddress;
    }

}