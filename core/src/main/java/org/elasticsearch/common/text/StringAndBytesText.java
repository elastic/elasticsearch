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

import com.google.common.base.Charsets;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Both {@link String} and {@link BytesReference} representation of the text. Starts with one of those, and if
 * the other is requests, caches the other one in a local reference so no additional conversion will be needed.
 */
public class StringAndBytesText implements Text {

    public static final Text[] EMPTY_ARRAY = new Text[0];

    public static Text[] convertFromStringArray(String[] strings) {
        if (strings.length == 0) {
            return EMPTY_ARRAY;
        }
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new StringAndBytesText(strings[i]);
        }
        return texts;
    }

    private BytesReference bytes;
    private String text;
    private int hash;

    public StringAndBytesText(BytesReference bytes) {
        this.bytes = bytes;
    }

    public StringAndBytesText(String text) {
        this.text = text;
    }

    @Override
    public boolean hasBytes() {
        return bytes != null;
    }

    @Override
    public BytesReference bytes() {
        if (bytes == null) {
            bytes = new BytesArray(text.getBytes(Charsets.UTF_8));
        }
        return bytes;
    }

    @Override
    public boolean hasString() {
        return text != null;
    }

    @Override
    public String string() {
        // TODO: we can optimize the conversion based on the bytes reference API similar to UnicodeUtil
        if (text == null) {
            if (!bytes.hasArray()) {
                bytes = bytes.toBytesArray();
            }
            text = new String(bytes.array(), bytes.arrayOffset(), bytes.length(), Charsets.UTF_8);
        }
        return text;
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
        if (obj == null) {
            return false;
        }
        return bytes().equals(((Text) obj).bytes());
    }

    @Override
    public int compareTo(Text text) {
        return UTF8SortedAsUnicodeComparator.utf8SortedAsUnicodeSortOrder.compare(bytes(), text.bytes());
    }
}
