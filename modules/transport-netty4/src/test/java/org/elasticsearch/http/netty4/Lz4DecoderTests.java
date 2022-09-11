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

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.elasticsearch.http.netty4.util.CompressOutputStream;
import org.elasticsearch.http.netty4.util.MockChannelHandlerContext;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoping
 * @date 2022/9/6
 */
public class Lz4DecoderTests extends ESTestCase {

    public void testLz4Decode() throws Exception {

        try (ByteArrayOutputStream compress = new ByteArrayOutputStream(1024)) {
            byte[] origin = randomBytes();
            try (OutputStream outputStream = CompressOutputStream.compressedOutputStream(compress)) {
                outputStream.write(origin);
            }
            byte[] bytes = compress.toByteArray();
            LZ4Decoder customDecompress = new LZ4Decoder();
            ByteArrayOutputStream decompress = new ByteArrayOutputStream(1024);
            ByteBuf in = Unpooled.wrappedBuffer(bytes);

            while (true) {
                List<Object> result = new ArrayList<>();

                customDecompress.decode(new MockChannelHandlerContext(), in, result);
                if (in.readableBytes() == 0) {
                    break;
                } else {
                    for (Object object : result) {
                        ByteBuf byteBuf = (ByteBuf) object;
                        byteBuf.readBytes(decompress, byteBuf.readableBytes());
                    }
                }
            }
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
