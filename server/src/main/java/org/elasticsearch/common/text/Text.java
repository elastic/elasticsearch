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
package org.elasticsearch.common.text;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Both {@link String} and {@link BytesReference} representation of the text. Starts with one of those, and if
 * the other is requests, caches the other one in a local reference so no additional conversion will be needed.
 */
public final class Text implements Comparable<Text>, ToXContentFragment {

    public static final Text[] EMPTY_ARRAY = new Text[0];

    public static Text[] convertFromStringArray(String[] strings) {
        if (strings.length == 0) {
            return EMPTY_ARRAY;
        }
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        return texts;
    }

    private BytesReference bytes;
    private String text;
    private int hash;

    public Text(BytesReference bytes) {
        this.bytes = bytes;
    }

    public Text(String text) {
        this.text = text;
    }

    /**
     * Whether a {@link BytesReference} view of the data is already materialized.
     */
    public boolean hasBytes() {
        return bytes != null;
    }

    /**
     * Returns a {@link BytesReference} view of the data.
     */
    public BytesReference bytes() {
        if (bytes == null) {
            bytes = new BytesArray(text.getBytes(StandardCharsets.UTF_8));
        }
        return bytes;
    }

    /**
     * Whether a {@link String} view of the data is already materialized.
     */
    public boolean hasString() {
        return text != null;
    }

    /**
     * Returns a {@link String} view of the data.
     */
    public String string() {
        return text == null ? bytes.utf8ToString() : text;
    }

    @Override
    public String toString() {
        return string();
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = bytes().hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return bytes().equals(((Text) obj).bytes());
    }

    @Override
    public int compareTo(Text text) {
        return bytes().compareTo(text.bytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasString()) {
            return builder.value(this.string());
        } else {
            // TODO: TextBytesOptimization we can use a buffer here to convert it? maybe add a
            // request to jackson to support InputStream as well?
            BytesRef br = this.bytes().toBytesRef();
            return builder.utf8Value(br.bytes, br.offset, br.length);
        }
    }
}
