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
 * A {@link String} only representation of the text. Will always convert to bytes on the fly.
 */
public class StringText implements Text {

    public static final Text[] EMPTY_ARRAY = new Text[0];

    public static Text[] convertFromStringArray(String[] strings) {
        if (strings.length == 0) {
            return EMPTY_ARRAY;
        }
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new StringText(strings[i]);
        }
        return texts;
    }

    private final String text;
    private int hash;

    public StringText(String text) {
        this.text = text;
    }

    @Override
    public boolean hasBytes() {
        return false;
    }

    @Override
    public BytesReference bytes() {
        return new BytesArray(text.getBytes(Charsets.UTF_8));
    }

    @Override
    public boolean hasString() {
        return true;
    }

    @Override
    public String string() {
        return text;
    }

    @Override
    public String toString() {
        return string();
    }

    @Override
    public int hashCode() {
        // we use bytes here so we can be consistent with other text implementations
        if (hash == 0) {
            hash = bytes().hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        // we use bytes here so we can be consistent with other text implementations
        return bytes().equals(((Text) obj).bytes());
    }

    @Override
    public int compareTo(Text text) {
        return UTF8SortedAsUnicodeComparator.utf8SortedAsUnicodeSortOrder.compare(bytes(), text.bytes());
    }
}
