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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class DocCountFieldMapperTests extends ESSingleNodeTestCase {

    private static final String CONTENT_TYPE = DocCountFieldMapper.CONTENT_TYPE;
    private static final String DOC_COUNT_FIELD = "doc_count";

    IndexService indexService;
    DocumentMapperParser parser;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper(DOC_COUNT_FIELD);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field(DOC_COUNT_FIELD, 10)
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        assertEquals(10L, doc.rootDoc().getField(DOC_COUNT_FIELD).numericValue());
    }

    public void testReadDocCounts() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(DOC_COUNT_FIELD);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field(DOC_COUNT_FIELD, 10)
                        .field("t", "5")
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        assertEquals(10L, doc.rootDoc().getField(DOC_COUNT_FIELD).numericValue());
    }

    /**
     * Test that invalid field mapping containing more than one doc_count fields
     */
    public void testInvalidMappingWithMultipleDocCounts() throws Exception {
        ensureGreen();
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .startObject("another_doc_count")
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> indexService.mapperService()
                .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(
            e.getMessage(),
            equalTo("Field [doc_count] conflicts with field [another_doc_count]. Only one field of type [doc_count] is allowed.")
        );
    }

    public void testInvalidDocument_NegativeDocCount() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(DOC_COUNT_FIELD);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field(DOC_COUNT_FIELD, -5)
                            .field("t", "5")
                            .endObject()
                    ), XContentType.JSON)));
        assertThat(e.getCause().getMessage(), containsString("Field [doc_count] must be a positive integer"));
    }


    public void testInvalidDocument_ZeroDocCount() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(DOC_COUNT_FIELD);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field(DOC_COUNT_FIELD, 0)
                            .field("t", "5")
                            .endObject()
                    ), XContentType.JSON)));
        assertThat(e.getCause().getMessage(), containsString("Field [doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_FractionalDocCount() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(DOC_COUNT_FIELD)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(DOC_COUNT_FIELD);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field(DOC_COUNT_FIELD, 100.23)
                            .field("t", "5")
                            .endObject()
                    ), XContentType.JSON)));
        assertThat(e.getCause().getMessage(), containsString("Field [doc_count] must be a positive integer"));
    }
}
