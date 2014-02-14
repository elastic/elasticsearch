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

package org.elasticsearch.benchmark.checksum;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.security.MessageDigest;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

/**
 *
 */
public class ChecksumBenchmark {

    public static final int BATCH_SIZE = 16 * 1024;

    public static void main(String[] args) throws Exception {
        System.out.println("Warning up");
        long warmSize = ByteSizeValue.parseBytesSizeValue("1g", null).bytes();
        crc(warmSize);
        adler(warmSize);
        md5(warmSize);

        long dataSize = ByteSizeValue.parseBytesSizeValue("10g", null).bytes();
        System.out.println("Running size: " + dataSize);
        crc(dataSize);
        adler(dataSize);
        md5(dataSize);
    }

    private static void crc(long dataSize) {
        long start = System.currentTimeMillis();
        CRC32 crc = new CRC32();
        byte[] data = new byte[BATCH_SIZE];
        long iter = dataSize / BATCH_SIZE;
        for (long i = 0; i < iter; i++) {
            crc.update(data);
        }
        crc.getValue();
        System.out.println("CRC took " + new TimeValue(System.currentTimeMillis() - start));
    }

    private static void adler(long dataSize) {
        long start = System.currentTimeMillis();
        Adler32 crc = new Adler32();
        byte[] data = new byte[BATCH_SIZE];
        long iter = dataSize / BATCH_SIZE;
        for (long i = 0; i < iter; i++) {
            crc.update(data);
        }
        crc.getValue();
        System.out.println("Adler took " + new TimeValue(System.currentTimeMillis() - start));
    }

    private static void md5(long dataSize) throws Exception {
        long start = System.currentTimeMillis();
        byte[] data = new byte[BATCH_SIZE];
        long iter = dataSize / BATCH_SIZE;
        MessageDigest digest = MessageDigest.getInstance("MD5");
        for (long i = 0; i < iter; i++) {
            digest.update(data);
        }
        digest.digest();
        System.out.println("md5 took " + new TimeValue(System.currentTimeMillis() - start));
    }
}
