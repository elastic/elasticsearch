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
import org.junit.Before;

import java.util.Collection;

public class RankFeatureMetaFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testBasics() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "rank_feature").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
        assertNotNull(mapper.metadataMapper(RankFeatureMetaFieldMapper.class));
    }

    /**
     * Check that meta-fields are picked from plugins (in this case MapperExtrasPlugin),
     * and parsing of a document fails if the document contains these meta-fields.
     */
    public void testDocumentParsingFailsOnMetaField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        String rfMetaField = RankFeatureMetaFieldMapper.CONTENT_TYPE;
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(rfMetaField, 0).endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertTrue(e.getMessage().contains("Field ["+ rfMetaField + "] is a metadata field and cannot be added inside a document."));
    }
}
