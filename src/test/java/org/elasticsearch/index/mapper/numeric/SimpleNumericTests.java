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

package org.elasticsearch.index.mapper.numeric;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleNumericTests extends ElasticsearchTestCase {

    @Test
    public void testNumericDetectionEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

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

    @Test
    public void testNumericDetectionDefault() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

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

    @Test
    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field1").field("type", "integer").field("ignore_malformed", true).endObject()
                    .startObject("field2").field("type", "integer").field("ignore_malformed", false).endObject()
                    .startObject("field3").field("type", "integer").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "a")
                .field("field2", "1")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field1"), nullValue());
        assertThat(doc.rootDoc().getField("field2"), notNullValue());

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Verify that the default is false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field3", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Unless the global ignore_malformed option is set to true
        Settings indexSettings = settingsBuilder().put("index.mapping.ignore_malformed", true).build();
        defaultMapper = MapperTestUtils.newParser(indexSettings).parse(mapping);
        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field3", "a")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field3"), nullValue());

        // This should still throw an exception, since field2 is specifically set to ignore_malformed=false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }
    }
}
