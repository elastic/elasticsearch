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

package org.elasticsearch.common.io;

import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.util.concurrent.NotThreadSafe;

import java.io.DataOutputStream;

/**
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public class ByteArrayDataOutputStream extends DataOutputStream {

    /**
     * A thread local based cache of {@link ByteArrayDataOutputStream}.
     */
    public static class Cached {

        private static final ThreadLocal<ThreadLocals.CleanableValue<ByteArrayDataOutputStream>> cache = new ThreadLocal<ThreadLocals.CleanableValue<ByteArrayDataOutputStream>>() {
            @Override protected ThreadLocals.CleanableValue<ByteArrayDataOutputStream> initialValue() {
                return new ThreadLocals.CleanableValue<ByteArrayDataOutputStream>(new ByteArrayDataOutputStream());
            }
        };

        /**
         * Returns the cached thread local byte strean, with its internal stream cleared.
         */
        public static ByteArrayDataOutputStream cached() {
            ByteArrayDataOutputStream os = cache.get().get();
            ((FastByteArrayOutputStream) os.out).reset();
            return os;
        }
    }


    public ByteArrayDataOutputStream() {
        super(new FastByteArrayOutputStream());
    }

    public byte[] copiedByteArray() {
        return outputStream().copiedByteArray();
    }

    public byte[] unsafeByteArray() {
        return outputStream().unsafeByteArray();
    }

    private FastByteArrayOutputStream outputStream() {
        return (FastByteArrayOutputStream) out;
    }
}
