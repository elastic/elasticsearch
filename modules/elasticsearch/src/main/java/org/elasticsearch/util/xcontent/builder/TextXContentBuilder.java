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

package org.elasticsearch.util.xcontent.builder;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.FastCharArrayWriter;
import org.elasticsearch.util.xcontent.XContent;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class TextXContentBuilder extends XContentBuilder<TextXContentBuilder> {

    private final FastCharArrayWriter writer;

    private final XContent xContent;

    final UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();

    public TextXContentBuilder(XContent xContent) throws IOException {
        this.writer = new FastCharArrayWriter();
        this.xContent = xContent;
        this.generator = xContent.createGenerator(writer);
        this.builder = this;
    }

    @Override public TextXContentBuilder raw(byte[] json) throws IOException {
        flush();
        Unicode.UTF16Result result = Unicode.unsafeFromBytesAsUtf16(json);
        writer.write(result.result, 0, result.length);
        return this;
    }

    public TextXContentBuilder reset() throws IOException {
        fieldCaseConversion = globalFieldCaseConversion;
        writer.reset();
        generator = xContent.createGenerator(writer);
        return this;
    }

    public String string() throws IOException {
        flush();
        return writer.toStringTrim();
    }

    public FastCharArrayWriter unsafeChars() throws IOException {
        flush();
        return writer;
    }

    @Override public byte[] unsafeBytes() throws IOException {
        return utf8().result;
    }

    /**
     * Call this AFTER {@link #unsafeBytes()}.
     */
    @Override public int unsafeBytesLength() {
        return utf8Result.length;
    }

    @Override public byte[] copiedBytes() throws IOException {
        flush();
        byte[] ret = new byte[utf8Result.length];
        System.arraycopy(utf8Result.result, 0, ret, 0, ret.length);
        return ret;
    }

    /**
     * Returns the byte[] that represents the utf8 of the json written up until now.
     * Note, the result is shared within this instance, so copy the byte array if needed
     * or use {@link #utf8copied()}.
     */
    public UnicodeUtil.UTF8Result utf8() throws IOException {
        flush();

        // ignore whitepsaces
        int st = 0;
        int len = writer.size();
        char[] val = writer.unsafeCharArray();

        while ((st < len) && (val[st] <= ' ')) {
            st++;
            len--;
        }
        while ((st < len) && (val[len - 1] <= ' ')) {
            len--;
        }

        UnicodeUtil.UTF16toUTF8(val, st, len, utf8Result);

        return utf8Result;
    }

    /**
     * Returns a copied byte[] that represnts the utf8 o fthe json written up until now.
     */
    public byte[] utf8copied() throws IOException {
        utf8();
        byte[] result = new byte[utf8Result.length];
        System.arraycopy(utf8Result.result, 0, result, 0, utf8Result.length);
        return result;
    }

}
