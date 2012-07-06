/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.index.mapper.numeric;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
@Test
public class SimpleNumericTests {

    public void testNumericDetectionEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(LongFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(DoubleFieldMapper.class));
    }

    public void testNumericDetectionDefault() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(StringFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(StringFieldMapper.class));
    }
}
