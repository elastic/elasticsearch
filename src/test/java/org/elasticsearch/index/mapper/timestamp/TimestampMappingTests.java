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

package org.elasticsearch.index.mapper.timestamp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 */
public class TimestampMappingTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp"), equalTo(null));
    }

    @Test
    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", "yes").field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp").fieldType().stored(), equalTo(true));
        assertThat(doc.rootDoc().getField("_timestamp").fieldType().indexed(), equalTo(true));
        assertThat(doc.rootDoc().getField("_timestamp").tokenStream(docMapper.indexAnalyzer(), null), notNullValue());
    }

    @Test
    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        assertThat(docMapper.timestampFieldMapper().enabled(), equalTo(TimestampFieldMapper.Defaults.ENABLED.enabled));
        assertThat(docMapper.timestampFieldMapper().fieldType().stored(), equalTo(TimestampFieldMapper.Defaults.FIELD_TYPE.stored()));
        assertThat(docMapper.timestampFieldMapper().fieldType().indexed(), equalTo(TimestampFieldMapper.Defaults.FIELD_TYPE.indexed()));
        assertThat(docMapper.timestampFieldMapper().path(), equalTo(null));
        assertThat(docMapper.timestampFieldMapper().dateTimeFormatter().format(), equalTo(TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT));
    }


    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                .field("enabled", "yes").field("store", "yes").field("index", "no")
                .field("path", "timestamp").field("format", "year")
                .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        assertThat(docMapper.timestampFieldMapper().enabled(), equalTo(true));
        assertThat(docMapper.timestampFieldMapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.timestampFieldMapper().fieldType().indexed(), equalTo(false));
        assertThat(docMapper.timestampFieldMapper().path(), equalTo("timestamp"));
        assertThat(docMapper.timestampFieldMapper().dateTimeFormatter().format(), equalTo("year"));
    }

    @Test
    public void testThatDisablingDuringMergeIsWorking() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper enabledMapper = MapperTestUtils.newParser().parse(enabledMapping);

        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper disabledMapper = MapperTestUtils.newParser().parse(disabledMapping);

        enabledMapper.merge(disabledMapper, DocumentMapper.MergeFlags.mergeFlags().simulate(false));

        assertThat(enabledMapper.timestampFieldMapper().enabled(), is(false));
    }

    @Test
    public void testThatDisablingFieldMapperDoesNotReturnAnyUselessInfo() throws Exception {
        boolean inversedStoreSetting = !TimestampFieldMapper.Defaults.FIELD_TYPE.stored();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false).field("store", inversedStoreSetting).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTestUtils.newParser().parse(mapping);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.timestampFieldMapper().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(builder.string(), is(String.format(Locale.ROOT, "{\"%s\":{}}", TimestampFieldMapper.NAME)));
    }

    @Test // issue 3174
    public void testThatSerializationWorksCorrectlyForIndexField() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("store", "yes").field("index", "no").endObject()
                .endObject().endObject().string();
        DocumentMapper enabledMapper = MapperTestUtils.newParser().parse(enabledMapping);

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        enabledMapper.timestampFieldMapper().toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap = JsonXContent.jsonXContent.createParser(builder.bytes()).mapAndClose();
        assertThat(serializedMap, hasKey("_timestamp"));
        assertThat(serializedMap.get("_timestamp"), instanceOf(Map.class));
        Map<String, Object> timestampConfiguration = (Map<String, Object>) serializedMap.get("_timestamp");
        assertThat(timestampConfiguration, hasKey("store"));
        assertThat(timestampConfiguration.get("store").toString(), is("true"));
        assertThat(timestampConfiguration, hasKey("index"));
        assertThat(timestampConfiguration.get("index").toString(), is("no"));
    }
}