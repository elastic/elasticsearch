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

package org.elasticsearch.util.json;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.io.FastByteArrayOutputStream;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BinaryJsonBuilder extends JsonBuilder<BinaryJsonBuilder> {

    /**
     * A thread local based cache of {@link BinaryJsonBuilder}.
     */
    public static class Cached {

        private BinaryJsonBuilder builder;

        public Cached(BinaryJsonBuilder builder) {
            this.builder = builder;
        }

        private static final ThreadLocal<Cached> cache = new ThreadLocal<Cached>() {
            @Override protected Cached initialValue() {
                try {
                    return new Cached(new BinaryJsonBuilder());
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create json generator", e);
                }
            }
        };

        /**
         * Returns the cached thread local generator, with its internal {@link StringBuilder} cleared.
         */
        static BinaryJsonBuilder cached() throws IOException {
            Cached cached = cache.get();
            cached.builder.reset();
            return cached.builder;
        }
    }

    private final FastByteArrayOutputStream bos;

    private final JsonFactory factory;

    public BinaryJsonBuilder() throws IOException {
        this(Jackson.defaultJsonFactory());
    }

    public BinaryJsonBuilder(JsonFactory factory) throws IOException {
        this.bos = new FastByteArrayOutputStream();
        this.factory = factory;
        this.generator = factory.createJsonGenerator(bos, JsonEncoding.UTF8);
        this.builder = this;
    }

    public BinaryJsonBuilder(JsonGenerator generator) throws IOException {
        this.bos = null;
        this.generator = generator;
        this.factory = null;
        this.builder = this;
    }

    @Override public BinaryJsonBuilder raw(byte[] json) throws IOException {
        flush();
        bos.write(json);
        return this;
    }

    @Override public BinaryJsonBuilder reset() throws IOException {
        bos.reset();
        generator = factory.createJsonGenerator(bos, JsonEncoding.UTF8);
        return this;
    }

    public FastByteArrayOutputStream unsafeStream() throws IOException {
        flush();
        return bos;
    }

    @Override public byte[] unsafeBytes() throws IOException {
        flush();
        return bos.unsafeByteArray();
    }

    @Override public int unsafeBytesLength() throws IOException {
        flush();
        return bos.size();
    }

    @Override public byte[] copiedBytes() throws IOException {
        flush();
        return bos.copiedByteArray();
    }
}
