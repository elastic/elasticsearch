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

package org.elasticsearch.common.xcontent.smile;

import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.jackson.JsonEncoding;
import org.elasticsearch.common.jackson.smile.SmileFactory;
import org.elasticsearch.common.jackson.smile.SmileGenerator;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * A JSON based content implementation using Jackson.
 *
 * @author kimchy (shay.banon)
 */
public class SmileXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(smileXContent);
    }

    final static SmileFactory smileFactory;
    public final static SmileXContent smileXContent;

    static {
        smileFactory = new SmileFactory();
        smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false); // for now, this is an overhead, might make sense for web sockets
        smileXContent = new SmileXContent();
    }

    private SmileXContent() {
    }

    @Override public XContentType type() {
        return XContentType.SMILE;
    }

    @Override public byte streamSeparator() {
        return (byte) 0xFF;
    }

    @Override public XContentGenerator createGenerator(OutputStream os) throws IOException {
        return new SmileXContentGenerator(smileFactory.createJsonGenerator(os, JsonEncoding.UTF8));
    }

    @Override public XContentGenerator createGenerator(Writer writer) throws IOException {
        return new SmileXContentGenerator(smileFactory.createJsonGenerator(writer));
    }

    @Override public XContentParser createParser(String content) throws IOException {
        return new SmileXContentParser(smileFactory.createJsonParser(new FastStringReader(content)));
    }

    @Override public XContentParser createParser(InputStream is) throws IOException {
        return new SmileXContentParser(smileFactory.createJsonParser(is));
    }

    @Override public XContentParser createParser(byte[] data) throws IOException {
        return new SmileXContentParser(smileFactory.createJsonParser(data));
    }

    @Override public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        return new SmileXContentParser(smileFactory.createJsonParser(data, offset, length));
    }

    @Override public XContentParser createParser(Reader reader) throws IOException {
        return new JsonXContentParser(smileFactory.createJsonParser(reader));
    }
}
