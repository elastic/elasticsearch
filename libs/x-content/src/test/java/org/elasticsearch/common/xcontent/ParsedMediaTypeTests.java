/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ParsedMediaTypeTests extends ESTestCase {

    public void testXContentTypes() {
        for (XContentType xContentType : XContentType.values()) {
            ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(xContentType.canonical());
            assertEquals(xContentType.canonical(), parsedMediaType.mimeTypeWithoutParams());
        }
    }

    public void testWithParameters() {
        String mediaType = "application/foo";
        assertEquals(Collections.emptyMap(), ParsedMediaType.parseMediaType(mediaType).getParameters());
        assertEquals(Collections.emptyMap(), ParsedMediaType.parseMediaType(mediaType + ";").getParameters());
        assertEquals(Map.of("charset", "utf-8"), ParsedMediaType.parseMediaType(mediaType + "; charset=UTF-8").getParameters());
        assertEquals(Map.of("charset", "utf-8", "compatible-with", "123"),
            ParsedMediaType.parseMediaType(mediaType + "; compatible-with=123;charset=UTF-8").getParameters());
    }

    public void testWhiteSpaces() {
        //be lenient with white space since it can be really hard to troubleshoot
        String mediaType = "  application/foo  ";
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaType + "    ;  compatible-with =  123  ;  charset=UTF-8");
        assertEquals("application/foo", parsedMediaType.mimeTypeWithoutParams());
        assertEquals((Map.of("charset", "utf-8", "compatible-with", "123")), parsedMediaType.getParameters());
    }

    public void testEmptyParams() {
        String mediaType = "application/foo";
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaType + randomFrom("", " ", ";", ";;", ";;;"));
        assertEquals("application/foo", parsedMediaType.mimeTypeWithoutParams());
        assertEquals(Collections.emptyMap(), parsedMediaType.getParameters());
    }

    public void testMalformedParameters() {
        String mediaType = "application/foo";
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "; charsetunknown"));
        assertThat(exception.getMessage(), equalTo("invalid parameters for header [application/foo; charsetunknown]"));

        exception = expectThrows(IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "; char=set=unknown"));
        assertThat(exception.getMessage(), equalTo("invalid parameters for header [application/foo; char=set=unknown]"));
    }
}
