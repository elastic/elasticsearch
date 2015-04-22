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
package org.elasticsearch.index.mapper.dynamic;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends ElasticsearchSingleNodeTest {

    public void testDynamicTrue() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }

    public void testDynamicFalse() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());
    }


    public void testDynamicStrict() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            defaultMapper.parse("type", "1", jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .field("field2", "value2")
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .field("field2", (String) null)
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }
    }

    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject().startObject("obj1")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            defaultMapper.parse("type", "1", jsonBuilder()
                    .startObject().startObject("obj1")
                    .field("field1", "value1")
                    .field("field2", "value2")
                    .endObject()
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        IndexService service = createIndex("test");
        client().prepareIndex("test", "type").setSource("empty_field", "").get();
        FieldMappers mappers = service.mapperService().fullName("empty_field");
        assertTrue(mappers != null && mappers.isEmpty() == false);
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_")
                .field("dynamic", "strict")
                .endObject().endObject();

        IndexService indexService = createIndex("test", ImmutableSettings.EMPTY, "_default_", mapping);

        try {
            client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test", "test").endObject()).get();
            fail();
        } catch (StrictDynamicMappingException e) {

        }

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNull(getMappingsResponse.getMappings().get("test").get("type"));
    }

    private String serialize(ToXContent mapper) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(ImmutableMap.<String, String>of()));
        return builder.endObject().string();
    }

    private Mapper parse(DocumentMapper mapper, DocumentMapperParser parser, XContentBuilder builder) throws Exception {
        Settings settings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        ParseContext.InternalParseContext ctx = new ParseContext.InternalParseContext("test", settings, parser, mapper, new ContentPath(0));
        SourceToParse source = SourceToParse.source(builder.bytes());
        ctx.reset(XContentHelper.createParser(source.source()), new ParseContext.Document(), source, null);
        assertEquals(XContentParser.Token.START_OBJECT, ctx.parser().nextToken());
        ctx.parser().nextToken();
        return mapper.root().parse(ctx);
    }

    public void testDynamicMappingsNotNeeded() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "string").endObject().endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        // foo is already defined in the mappings
        assertNull(update);
    }

    public void testField() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals("{\"type\":{\"properties\":{\"foo\":{\"type\":\"string\"}}}}", serialize(update));
    }

    public void testIncremental() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        // Make sure that mapping updates are incremental, this is important for performance otherwise
        // every new field introduction runs in linear time with the total number of fields
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "string").endObject().endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                // foo is NOT in the update
                .startObject("bar").field("type", "string").endObject()
                .endObject().endObject().string(), serialize(update));
    }

    public void testIntroduceTwoFields() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("bar").field("type", "string").endObject()
                .startObject("foo").field("type", "string").endObject()
                .endObject().endObject().string(), serialize(update));
    }

    public void testObject() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "string").endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo").value("bar").value("baz").endArray().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").field("type", "string").endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testInnerDynamicMapping() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type") .startObject("properties")
                .startObject("foo").field("type", "object").endObject()
                .endObject().endObject().endObject().string();
        
        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        // original mapping not modified
        assertEquals(mapping, serialize(mapper));
        // but we have an update
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "string").endObject().endObject().endObject().endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }

    public void testComplexArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").endObject()
                .endObject().string();

        DocumentMapper mapper = parser.parse(mapping);
        assertEquals(mapping, serialize(mapper));

        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo")
                .startObject().field("bar", "baz").endObject()
                .startObject().field("baz", 3).endObject()
                .endArray().endObject());
        assertEquals(mapping, serialize(mapper));
        assertEquals(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").startObject("properties")
                .startObject("bar").field("type", "string").endObject()
                .startObject("baz").field("type", "long").endObject()
                .endObject().endObject()
                .endObject().endObject().endObject().string(), serialize(update));
    }
}
