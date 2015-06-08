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

package org.elasticsearch.common.xcontent.cbor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.BaseJsonGenerator;
import org.elasticsearch.common.xcontent.support.filtering.FilteringJsonGenerator;

import java.io.*;

/**
 * A CBOR based content implementation using Jackson.
 */
public class CborXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(cborXContent);
    }

    final static CBORFactory cborFactory;
    public final static CborXContent cborXContent;

    static {
        cborFactory = new CBORFactory();
        cborFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        cborXContent = new CborXContent();
    }

    private CborXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.CBOR;
    }

    @Override
    public byte streamSeparator() {
        throw new ElasticsearchParseException("cbor does not support stream parsing...");
    }

    private XContentGenerator newXContentGenerator(JsonGenerator jsonGenerator) {
        return new CborXContentGenerator(new BaseJsonGenerator(jsonGenerator));
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os) throws IOException {
        return newXContentGenerator(cborFactory.createGenerator(os, JsonEncoding.UTF8));
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, String[] filters) throws IOException {
        if (CollectionUtils.isEmpty(filters)) {
            return createGenerator(os);
        }
        FilteringJsonGenerator cborGenerator = new FilteringJsonGenerator(cborFactory.createGenerator(os, JsonEncoding.UTF8), filters);
        return new CborXContentGenerator(cborGenerator);
    }

    @Override
    public XContentGenerator createGenerator(Writer writer) throws IOException {
        return newXContentGenerator(cborFactory.createGenerator(writer));
    }

    @Override
    public XContentParser createParser(String content) throws IOException {
        return new CborXContentParser(cborFactory.createParser(new FastStringReader(content)));
    }

    @Override
    public XContentParser createParser(InputStream is) throws IOException {
        return new CborXContentParser(cborFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(byte[] data) throws IOException {
        return new CborXContentParser(cborFactory.createParser(data));
    }

    @Override
    public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        return new CborXContentParser(cborFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(BytesReference bytes) throws IOException {
        if (bytes.hasArray()) {
            return createParser(bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        return createParser(bytes.streamInput());
    }

    @Override
    public XContentParser createParser(Reader reader) throws IOException {
        return new CborXContentParser(cborFactory.createParser(reader));
    }
}
