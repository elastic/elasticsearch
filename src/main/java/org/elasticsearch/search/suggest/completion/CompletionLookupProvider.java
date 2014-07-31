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

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class CompletionLookupProvider implements PayloadProcessor, CompletionTokenStream.ToFiniteStrings {

    public static final char UNIT_SEPARATOR = '\u001f';

    public abstract FieldsConsumer consumer(IndexOutput output) throws IOException;

    public abstract String getName();

    public abstract LookupFactory load(IndexInput input) throws IOException;

    @Override
    public BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException {
        if (weight < -1 || weight > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("weight must be >= -1 && <= Integer.MAX_VALUE");
        }
        for (int i = 0; i < surfaceForm.length; i++) {
            if (surfaceForm.bytes[i] == UNIT_SEPARATOR) {
                throw new IllegalArgumentException(
                        "surface form cannot contain unit separator character U+001F; this character is reserved");
            }
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream);
        output.writeVLong(weight + 1);
        output.writeVInt(surfaceForm.length);
        output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
        output.writeVInt(payload.length);
        output.writeBytes(payload.bytes, 0, payload.length);

        output.close();
        return new BytesRef(byteArrayOutputStream.toByteArray());
    }

    @Override
    public void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
        InputStreamDataInput input = new InputStreamDataInput(byteArrayInputStream);
        ref.weight = input.readVLong() - 1;
        int len = input.readVInt();
        ref.surfaceForm.grow(len);
        ref.surfaceForm.length = len;
        input.readBytes(ref.surfaceForm.bytes, ref.surfaceForm.offset, ref.surfaceForm.length);
        len = input.readVInt();
        ref.payload.grow(len);
        ref.payload.length = len;
        input.readBytes(ref.payload.bytes, ref.payload.offset, ref.payload.length);
        input.close();
    }
}
