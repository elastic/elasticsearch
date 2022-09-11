/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.client.RestClient.ByteArrayInputOutputStream;
import org.elasticsearch.client.util.LZ4Decompress;

import java.io.OutputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author xiaoping
 * @date 2022/9/6
 */
public class ReuseBuffersLZ4BlockOutputStreamTests extends RestClientTestCase {
    public void testLz4Compression() throws Exception {
        try (ByteArrayInputOutputStream compress = new ByteArrayInputOutputStream(1024)) {
            byte[] origin = randomBytes();
            try (OutputStream outputStream = ReuseBuffersLZ4BlockOutputStream.compressedOutputStream(compress)) {
                outputStream.write(origin);
            }
            byte[] bytes = compress.toByteArray();
            LZ4Decompress customDecompress = new LZ4Decompress();
            ByteArrayInputOutputStream decompress = new ByteArrayInputOutputStream(1024);
            customDecompress.decode(bytes, decompress);
            byte[] decompressBytes = decompress.toByteArray();
            assertEquals(origin.length, decompressBytes.length);
            for (int i = 0; i < origin.length; i++) {
                assertEquals(origin[i], decompressBytes[i]);
            }
            decompress.close();
        }
    }

    public byte[] randomBytes() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < randomIntBetween(1000, 2000); i++) {
            int value = randomInt();
            for (int j = 0; j < randomIntBetween(50, 100); j++) {
                stringBuilder.append(value);
            }
        }
        return stringBuilder.toString().getBytes();
    }
}
