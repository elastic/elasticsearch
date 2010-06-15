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

package org.elasticsearch.common.xcontent.builder;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.FastByteArrayOutputStream;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BinaryXContentBuilder extends XContentBuilder<BinaryXContentBuilder> {

    private final FastByteArrayOutputStream bos;

    private final XContent xContent;

    public BinaryXContentBuilder(XContent xContent) throws IOException {
        this.bos = new FastByteArrayOutputStream();
        this.xContent = xContent;
        this.generator = xContent.createGenerator(bos);
        this.builder = this;
    }

    @Override public BinaryXContentBuilder raw(byte[] json) throws IOException {
        flush();
        bos.write(json);
        return this;
    }

    @Override public BinaryXContentBuilder reset() throws IOException {
        fieldCaseConversion = globalFieldCaseConversion;
        bos.reset();
        this.generator = xContent.createGenerator(bos);
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

    @Override public String string() throws IOException {
        flush();
        return Unicode.fromBytes(bos.unsafeByteArray(), 0, bos.size());
    }
}
