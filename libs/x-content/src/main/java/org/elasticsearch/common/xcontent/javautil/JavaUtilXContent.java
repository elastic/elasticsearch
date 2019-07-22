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

package org.elasticsearch.common.xcontent.javautil;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

/**
 * Plain Java Map and List XContent.
 */
public class JavaUtilXContent implements XContent {

    public static final JavaUtilXContent javaUtilXContent;

    static {
        javaUtilXContent = new JavaUtilXContent();
    }

    private JavaUtilXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.JSON;
    }

    @Override
    public byte streamSeparator() {
        return '\n';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new JavaUtilXContentGenerator();
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                       DeprecationHandler deprecationHandler, String content) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                       DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                       DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                       DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return null;
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                       DeprecationHandler deprecationHandler, Reader reader) throws IOException {
        return null;
    }
}
