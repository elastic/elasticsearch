/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.io.compressor;

import org.elasticsearch.util.io.compression.Compressor;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractCompressorTests {

    private static final String TEST_STRING = "aaaaaaaaaaaa bbbbbbbbbb aa aa aa cccccccccc";

    @Test public void testSimpleOperations() throws Exception {
        Compressor compressor = createCompressor();
        byte[] compressed = compressor.compressString(TEST_STRING);
        System.out.println("" + TEST_STRING.length());
        System.out.println("" + compressed.length);

        assertThat(compressed.length, lessThan(TEST_STRING.length()));

        String decompressed = compressor.decompressString(compressed);
//        System.out.println("" + TEST_STRING.length());
//        System.out.println("" + compressed.length);
        assertThat(decompressed, equalTo(TEST_STRING));

        decompressed = compressor.decompressString(compressed);
        assertThat(decompressed, equalTo(TEST_STRING));

        compressed = compressor.compressString(TEST_STRING);
//        System.out.println("" + TEST_STRING.length());
//        System.out.println("" + compressed.length);
        assertThat(compressed.length, lessThan(TEST_STRING.length()));

        decompressed = compressor.decompressString(compressed);
        assertThat(decompressed, equalTo(TEST_STRING));

        decompressed = compressor.decompressString(compressed);
        assertThat(decompressed, equalTo(TEST_STRING));
    }

    protected abstract Compressor createCompressor();
}
