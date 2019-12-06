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

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocumentMapperParserTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testTypeLevel() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertThat(mapper.type(), equalTo("type"));
    }

    public void testFieldNameWithDots() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo.bar").field("type", "text").endObject()
            .startObject("foo.baz").field("type", "keyword").endObject()
            .endObject().endObject().endObject());
        DocumentMapper docMapper = mapperParser.parse("type", new CompressedXContent(mapping));
        assertNotNull(docMapper.mappers().getMapper("foo.bar"));
        assertNotNull(docMapper.mappers().getMapper("foo.baz"));
        assertNotNull(docMapper.objectMappers().get("foo"));
    }

    public void testFieldNameWithDeepDots() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo.bar").field("type", "text").endObject()
            .startObject("foo.baz").startObject("properties")
            .startObject("deep.field").field("type", "keyword").endObject().endObject()
            .endObject().endObject().endObject().endObject());
        DocumentMapper docMapper = mapperParser.parse("type", new CompressedXContent(mapping));
        assertNotNull(docMapper.mappers().getMapper("foo.bar"));
        assertNotNull(docMapper.mappers().getMapper("foo.baz.deep.field"));
        assertNotNull(docMapper.objectMappers().get("foo"));
    }

    public void testFieldNameWithDotsConflict() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("type", "text").endObject()
            .startObject("foo.baz").field("type", "keyword").endObject()
            .endObject().endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            mapperParser.parse("type", new CompressedXContent(mapping)));
        assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] of different type"));
    }

    public void testFieldWithLeadingDot() throws Exception {
        for (Version indexVersion : Arrays.asList(Version.V_7_5_0, Version.V_8_0_0)) {
            IndexService indexService = createTestIndex(indexVersion);
            DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();

            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject(".field")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                mapperParser.parse("_doc", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("An object field's name cannot be empty."));
        }
    }

    public void testFieldWithSingleDot() throws Exception {
        for (Version indexVersion : Arrays.asList(Version.V_7_5_0, Version.V_8_0_0)) {
            IndexService indexService = createTestIndex(indexVersion);
            DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();

            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject(".")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                mapperParser.parse("_doc", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("A field's name cannot be empty."));
        }
    }

    public void testFieldWithTrailingDot() throws Exception {
        IndexService indexService = createTestIndex(Version.V_8_0_0);
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("field.")
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            mapperParser.parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("A field's name cannot be empty."));
    }

    // Prior to 8.0, we accepted these field names with trailing dots but stripped them away. Here we
    // ensure that they are still accepted, to maintain backwards compatibility with 7.x indices.
    public void testFieldWithTrailingDot_V_7() throws Exception {
        Version indexVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        IndexService indexService = createTestIndex(indexVersion);
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("field.")
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = mapperParser.parse("_doc", new CompressedXContent(mapping));
        assertNotNull(mapper.mappers().getMapper("field"));
        assertWarnings(ObjectMapper.TRAILING_DOT_DEPRECATION_MESSAGE);
    }

    public void testMultiFieldsWithFieldAlias() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("field")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("alias")
                            .field("type", "alias")
                            .field("path", "other-field")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("other-field")
                    .field("type", "keyword")
                .endObject()
            .endObject()
        .endObject().endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapperParser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Type [alias] cannot be used in multi field", e.getMessage());
    }

    private IndexService createTestIndex(Version indexVersion) {
        Settings settings = Settings.builder()
            .put("index.version.created", indexVersion)
            .build();
        return createIndex("test-" + indexVersion, settings);
    }
}
