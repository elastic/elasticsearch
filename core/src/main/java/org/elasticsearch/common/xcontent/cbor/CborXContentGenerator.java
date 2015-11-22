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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class CborXContentGenerator extends JsonXContentGenerator {

    public CborXContentGenerator(JsonGenerator jsonGenerator, String... filters) {
        super(jsonGenerator, filters);
    }

    @Override
    public XContentType contentType() {
        return XContentType.CBOR;
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        // nothing here
    }

    @Override
    public void writeRawField(String fieldName, InputStream content, OutputStream bos, XContentType contentType) throws IOException {
        writeFieldName(fieldName);
        try (CBORParser parser = CborXContent.cborFactory.createParser(content)) {
            parser.nextToken();
            generator.copyCurrentStructure(parser);
        }
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, OutputStream bos) throws IOException {
        writeFieldName(fieldName);
        try (CBORParser parser = CborXContent.cborFactory.createParser(content)) {
            parser.nextToken();
            generator.copyCurrentStructure(parser);
        }
    }

    @Override
    protected void writeObjectRaw(String fieldName, BytesReference content, OutputStream bos) throws IOException {
        writeFieldName(fieldName);
        CBORParser parser;
        if (content.hasArray()) {
            parser = CborXContent.cborFactory.createParser(content.array(), content.arrayOffset(), content.length());
        } else {
            parser = CborXContent.cborFactory.createParser(content.streamInput());
        }
        try {
            parser.nextToken();
            generator.copyCurrentStructure(parser);
        } finally {
            parser.close();
        }
    }

    @Override
    public void writeRawField(String fieldName, byte[] content, int offset, int length, OutputStream bos) throws IOException {
        writeFieldName(fieldName);
        try (CBORParser parser = CborXContent.cborFactory.createParser(content, offset, length)) {
            parser.nextToken();
            generator.copyCurrentStructure(parser);
        }
    }
}
