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

package org.elasticsearch.common.xcontent.builder;

import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;

public class XContentBuilderNumericTests extends ElasticsearchTestCase {

    @Test
    public void testBigInteger() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("value", new BigInteger("1234567891234567890123456789")).endObject();
        assertThat(builder.string(), equalTo("{\"value\":1234567891234567890123456789}"));

        XContentType contentType = XContentFactory.xContentType(builder.string());
        Map<String,Object> map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(true)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=1234567891234567890123456789}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.math.BigInteger"));

        map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(false)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=1234567891234567890123456789}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.math.BigInteger"));
    }

    @Test
    public void testLosslessDecimal() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("value", new BigInteger("1234")).endObject();
        assertThat(builder.string(), equalTo("{\"value\":1234}"));

        XContentType contentType = XContentFactory.xContentType(builder.string());
        Map<String, Object> map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(true)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=1234}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.math.BigInteger"));

        map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(false)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=1234}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.lang.Integer"));

        builder = XContentFactory.jsonBuilder();
        builder.startObject().field("value", new BigDecimal("12.34")).endObject();
        assertThat(builder.string(), equalTo("{\"value\":12.34}"));

        contentType = XContentFactory.xContentType(builder.string());
        map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(true)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=12.34}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.math.BigDecimal"));

        map = XContentFactory.xContent(contentType)
                .createParser(builder.string())
                .losslessDecimals(false)
                .mapAndClose();
        assertThat(map.toString(), equalTo("{value=12.34}"));
        assertThat(map.get("value").getClass().toString(), equalTo("class java.lang.Double"));
    }
}
