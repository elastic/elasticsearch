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

package org.elasticsearch.util.xcontent;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;
import org.elasticsearch.util.xcontent.builder.TextXContentBuilder;
import org.elasticsearch.util.xcontent.json.JsonXContent;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class XContentFactory {

    private static int GUESS_HEADER_LENGTH = 20;

    private static final XContent[] contents;

    static {
        contents = new XContent[1];
        contents[0] = new JsonXContent();
    }

    public static BinaryXContentBuilder jsonBuilder() throws IOException {
        return contentBinaryBuilder(XContentType.JSON);
    }

    public static BinaryXContentBuilder contentBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBinaryBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    public static BinaryXContentBuilder contentBinaryBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBinaryBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    public static TextXContentBuilder contentTextBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentTextBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    public static XContent xContent(XContentType type) {
        return contents[type.index()];
    }

    public static XContentType xContentType(CharSequence content) {
        int length = content.length() < GUESS_HEADER_LENGTH ? content.length() : GUESS_HEADER_LENGTH;
        for (int i = 0; i < length; i++) {
            char c = content.charAt(i);
            if (c == '{') {
                return XContentType.JSON;
            }
        }
        throw new ElasticSearchIllegalStateException("Failed to derive xContent from byte stream");
    }

    public static XContent xContent(CharSequence content) {
        return xContent(xContentType(content));
    }

    public static XContent xContent(byte[] data) {
        return xContent(data, 0, data.length);
    }

    public static XContent xContent(byte[] data, int offset, int length) {
        return xContent(xContentType(data, offset, length));
    }

    public static XContentType xContentType(byte[] data) {
        return xContentType(data, 0, data.length);
    }

    public static XContentType xContentType(byte[] data, int offset, int length) {
        length = length < GUESS_HEADER_LENGTH ? length : GUESS_HEADER_LENGTH;
        for (int i = offset; i < length; i++) {
            if (data[i] == '{') {
                return XContentType.JSON;
            }
        }
        throw new ElasticSearchIllegalStateException("Failed to derive xContent from byte stream");
    }
}
