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

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class DocCountFieldMapperTests extends ESSingleNodeTestCase {

    private static final String CONTENT_TYPE = DocCountFieldMapper.CONTENT_TYPE;
    private static final String FIELD_NAME = "doc_count";

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
                .startObject(FIELD_NAME)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field(FIELD_NAME, 10)
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        assertEquals(10L, doc.rootDoc().getField(FIELD_NAME).numericValue());
    }

    public void testReadDocCounts() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject(FIELD_NAME)
                .field("type", CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper defaultMapper = parser.parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(DocCountFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field(FIELD_NAME, 10)
                        .field("t", "5")
                        .endObject()
                ),
                XContentType.JSON
            )
        );
        assertEquals(10L, doc.rootDoc().getField(FIELD_NAME).numericValue());
    }

}
