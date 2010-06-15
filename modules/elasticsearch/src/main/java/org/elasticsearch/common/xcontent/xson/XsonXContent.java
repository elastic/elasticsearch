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

package org.elasticsearch.common.xcontent.xson;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.io.FastByteArrayInputStream;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.builder.BinaryXContentBuilder;

import java.io.*;

/**
 * A binary representation of content (basically, JSON encoded in optimized binary format).
 *
 * @author kimchy (shay.banon)
 */
public class XsonXContent implements XContent {

    public static class CachedBinaryBuilder {

        private static final ThreadLocal<ThreadLocals.CleanableValue<BinaryXContentBuilder>> cache = new ThreadLocal<ThreadLocals.CleanableValue<BinaryXContentBuilder>>() {
            @Override protected ThreadLocals.CleanableValue<BinaryXContentBuilder> initialValue() {
                try {
                    BinaryXContentBuilder builder = new BinaryXContentBuilder(new XsonXContent());
                    return new ThreadLocals.CleanableValue<BinaryXContentBuilder>(builder);
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create xson generator", e);
                }
            }
        };

        /**
         * Returns the cached thread local generator, with its internal {@link StringBuilder} cleared.
         */
        static BinaryXContentBuilder cached() throws IOException {
            ThreadLocals.CleanableValue<BinaryXContentBuilder> cached = cache.get();
            cached.get().reset();
            return cached.get();
        }
    }

    public static BinaryXContentBuilder contentBinaryBuilder() throws IOException {
        return CachedBinaryBuilder.cached();
    }

    @Override public XContentType type() {
        return XContentType.XSON;
    }

    @Override public XContentGenerator createGenerator(OutputStream os) throws IOException {
        return new XsonXContentGenerator(os);
    }

    @Override public XContentGenerator createGenerator(Writer writer) throws IOException {
        throw new ElasticSearchIllegalStateException("Can't create generator over xson with textual data");
    }

    @Override public XContentParser createParser(String content) throws IOException {
        throw new ElasticSearchIllegalStateException("Can't create parser over xson for textual data");
    }

    @Override public XContentParser createParser(InputStream is) throws IOException {
        return new XsonXContentParser(is);
    }

    @Override public XContentParser createParser(byte[] data) throws IOException {
        return new XsonXContentParser(new FastByteArrayInputStream(data));
    }

    @Override public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        return new XsonXContentParser(new FastByteArrayInputStream(data, offset, length));
    }

    @Override public XContentParser createParser(Reader reader) throws IOException {
        throw new ElasticSearchIllegalStateException("Can't create parser over xson for textual data");
    }
}
