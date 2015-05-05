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

package org.elasticsearch.common.xcontent.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BaseJsonGenerator extends JsonGeneratorDelegate {

    protected final GeneratorBase base;

    public BaseJsonGenerator(JsonGenerator generator, JsonGenerator base) {
        super(generator, true);
        if (base instanceof GeneratorBase) {
            this.base = (GeneratorBase) base;
        } else {
            this.base = null;
        }
    }

    public BaseJsonGenerator(JsonGenerator generator) {
        this(generator, generator);
    }

    protected void writeStartRaw(String fieldName) throws IOException {
        writeFieldName(fieldName);
        writeRaw(':');
    }

    public void writeEndRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + delegate.getClass();
        if (base != null) {
            base.getOutputContext().writeValue();
        }
    }

    protected void writeRawValue(byte[] content, OutputStream bos) throws IOException {
        flush();
        bos.write(content);
    }

    protected void writeRawValue(byte[] content, int offset, int length, OutputStream bos) throws IOException {
        flush();
        bos.write(content, offset, length);
    }

    protected void writeRawValue(InputStream content, OutputStream bos) throws IOException {
        flush();
        Streams.copy(content, bos);
    }

    protected void writeRawValue(BytesReference content, OutputStream bos) throws IOException {
        flush();
        content.writeTo(bos);
    }
}
