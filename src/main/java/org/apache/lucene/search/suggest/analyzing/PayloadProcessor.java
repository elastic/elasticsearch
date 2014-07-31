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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class PayloadProcessor<W> {

    private final BytesRefBuilder scratch = new BytesRefBuilder();

    private BytesRef surfaceForm = null;
    private W weight = null;

    public void parse(BytesRef payload) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
        InputStreamDataInput input = new InputStreamDataInput(byteArrayInputStream);
        int len = input.readVInt();
        scratch.grow(len);
        scratch.setLength(len);
        input.readBytes(scratch.bytes(), 0, scratch.length());
        surfaceForm = scratch.get();
        weight = parseWeight(input);
        input.close();
    }

    public BytesRef build(BytesRef surfaceForm, W weight) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream);
        output.writeVInt(surfaceForm.length);
        output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
        writeWeight(output, weight);
        output.close();
        return new BytesRef(byteArrayOutputStream.toByteArray());
    }

    public W weight() {
        return weight;
    }

    public BytesRef surfaceForm() {
        return surfaceForm;
    }

    abstract W parseWeight(InputStreamDataInput input) throws IOException;

    abstract void writeWeight(OutputStreamDataOutput output, W weight) throws IOException;

    public final static class BytesRefPayloadProcessor extends PayloadProcessor<BytesRef> {
        private final BytesRefBuilder scratch = new BytesRefBuilder();

        @Override
        BytesRef parseWeight(InputStreamDataInput input) throws IOException {
            int len = input.readVInt();
            scratch.grow(len);
            scratch.setLength(len);
            input.readBytes(scratch.bytes(), 0, scratch.length());
            return scratch.get();
        }

        @Override
        void writeWeight(OutputStreamDataOutput output, BytesRef weight) throws IOException {
            output.writeVInt(weight.length);
            output.writeBytes(weight.bytes, weight.offset, weight.length);
        }
    }

    public final static class LongPayloadProcessor extends PayloadProcessor<Long> {

        @Override
        Long parseWeight(InputStreamDataInput input) throws IOException {
            return input.readVLong() - 1;
        }

        @Override
        void writeWeight(OutputStreamDataOutput output, Long weight) throws IOException {
            if (weight < -1 || weight > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("weight must be >= -1 && <= Integer.MAX_VALUE");
            }
            output.writeVLong(weight + 1);
        }

    }
}
