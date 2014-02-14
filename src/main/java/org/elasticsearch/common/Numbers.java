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

import org.apache.lucene.util.BytesRef;

/**
 * A set of utilities for numbers.
 */
public final class Numbers {

    private Numbers() {

    }

    /**
     * Converts a byte array to an short.
     *
     * @param arr The byte array to convert to an short
     * @return The int converted
     */
    public static short bytesToShort(byte[] arr) {
        return (short) (((arr[0] & 0xff) << 8) | (arr[1] & 0xff));
    }

    public static short bytesToShort(BytesRef bytes) {
        return (short) (((bytes.bytes[bytes.offset] & 0xff) << 8) | (bytes.bytes[bytes.offset + 1] & 0xff));
    }

    /**
     * Converts a byte array to an int.
     *
     * @param arr The byte array to convert to an int
     * @return The int converted
     */
    public static int bytesToInt(byte[] arr) {
        return (arr[0] << 24) | ((arr[1] & 0xff) << 16) | ((arr[2] & 0xff) << 8) | (arr[3] & 0xff);
    }

    public static int bytesToInt(BytesRef bytes) {
        return (bytes.bytes[bytes.offset] << 24) | ((bytes.bytes[bytes.offset + 1] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 2] & 0xff) << 8) | (bytes.bytes[bytes.offset + 3] & 0xff);
    }

    /**
     * Converts a byte array to a long.
     *
     * @param arr The byte array to convert to a long
     * @return The long converter
     */
    public static long bytesToLong(byte[] arr) {
        int high = (arr[0] << 24) | ((arr[1] & 0xff) << 16) | ((arr[2] & 0xff) << 8) | (arr[3] & 0xff);
        int low = (arr[4] << 24) | ((arr[5] & 0xff) << 16) | ((arr[6] & 0xff) << 8) | (arr[7] & 0xff);
        return (((long) high) << 32) | (low & 0x0ffffffffL);
    }

    public static long bytesToLong(BytesRef bytes) {
        int high = (bytes.bytes[bytes.offset + 0] << 24) | ((bytes.bytes[bytes.offset + 1] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 2] & 0xff) << 8) | (bytes.bytes[bytes.offset + 3] & 0xff);
        int low = (bytes.bytes[bytes.offset + 4] << 24) | ((bytes.bytes[bytes.offset + 5] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 6] & 0xff) << 8) | (bytes.bytes[bytes.offset + 7] & 0xff);
        return (((long) high) << 32) | (low & 0x0ffffffffL);
    }

    /**
     * Converts a byte array to float.
     *
     * @param arr The byte array to convert to a float
     * @return The float converted
     */
    public static float bytesToFloat(byte[] arr) {
        return Float.intBitsToFloat(bytesToInt(arr));
    }

    public static float bytesToFloat(BytesRef bytes) {
        return Float.intBitsToFloat(bytesToInt(bytes));
    }

    /**
     * Converts a byte array to double.
     *
     * @param arr The byte array to convert to a double
     * @return The double converted
     */
    public static double bytesToDouble(byte[] arr) {
        return Double.longBitsToDouble(bytesToLong(arr));
    }

    public static double bytesToDouble(BytesRef bytes) {
        return Double.longBitsToDouble(bytesToLong(bytes));
    }

    /**
     * Converts an int to a byte array.
     *
     * @param val The int to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] intToBytes(int val) {
        byte[] arr = new byte[4];
        arr[0] = (byte) (val >>> 24);
        arr[1] = (byte) (val >>> 16);
        arr[2] = (byte) (val >>> 8);
        arr[3] = (byte) (val);
        return arr;
    }

    /**
     * Converts an int to a byte array.
     *
     * @param val The int to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] shortToBytes(int val) {
        byte[] arr = new byte[2];
        arr[0] = (byte) (val >>> 8);
        arr[1] = (byte) (val);
        return arr;
    }

    /**
     * Converts a long to a byte array.
     *
     * @param val The long to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] longToBytes(long val) {
        byte[] arr = new byte[8];
        arr[0] = (byte) (val >>> 56);
        arr[1] = (byte) (val >>> 48);
        arr[2] = (byte) (val >>> 40);
        arr[3] = (byte) (val >>> 32);
        arr[4] = (byte) (val >>> 24);
        arr[5] = (byte) (val >>> 16);
        arr[6] = (byte) (val >>> 8);
        arr[7] = (byte) (val);
        return arr;
    }

    /**
     * Converts a float to a byte array.
     *
     * @param val The float to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] floatToBytes(float val) {
        return intToBytes(Float.floatToRawIntBits(val));
    }

    /**
     * Converts a double to a byte array.
     *
     * @param val The double to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] doubleToBytes(double val) {
        return longToBytes(Double.doubleToRawLongBits(val));
    }

}
