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

package org.elasticsearch.index.percolator;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PercolatorFieldMapperTests extends ESSingleNodeTestCase {

    private MapperService mapperService;

    @Before
    public void init() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        mapperService = indexService.mapperService();

        String mapper = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", "text").endObject().endObject()
            .endObject().endObject().string();
        mapperService.merge("type", new CompressedXContent(mapper), MapperService.MergeReason.MAPPING_UPDATE, true);

        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(PercolatorService.TYPE_NAME)
            .startObject("properties").startObject("query").field("type", "percolator").endObject().endObject()
            .endObject().endObject().string();
        mapperService.merge(PercolatorService.TYPE_NAME, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
    }

    public void testPercolatorFieldMapper() throws Exception {
        ParsedDocument doc = mapperService.documentMapper(PercolatorService.TYPE_NAME).parse("test", PercolatorService.TYPE_NAME, "1", XContentFactory.jsonBuilder().startObject()
            .field("query", termQuery("field", "value"))
            .endObject().bytes());

        assertThat(doc.rootDoc().getFields(PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME).length, equalTo(1));
        assertThat(doc.rootDoc().getFields(PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME)[0].binaryValue().utf8ToString(), equalTo("field\0value"));
    }

    public void testPercolatorFieldMapper_noQuery() throws Exception {
        ParsedDocument doc = mapperService.documentMapper(PercolatorService.TYPE_NAME).parse("test", PercolatorService.TYPE_NAME, "1", XContentFactory.jsonBuilder().startObject()
            .endObject().bytes());
        assertThat(doc.rootDoc().getFields(PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME).length, equalTo(0));

        try {
            mapperService.documentMapper(PercolatorService.TYPE_NAME).parse("test", PercolatorService.TYPE_NAME, "1", XContentFactory.jsonBuilder().startObject()
                .nullField("query")
                .endObject().bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("query malformed, must start with start_object"));
        }
    }

    public void testAllowNoAdditionalSettings() throws Exception {
        IndexService indexService = createIndex("test1", Settings.EMPTY);
        MapperService mapperService = indexService.mapperService();

        String percolatorMapper = XContentFactory.jsonBuilder().startObject().startObject(PercolatorService.TYPE_NAME)
            .startObject("properties").startObject("query").field("type", "percolator").field("index", "no").endObject().endObject()
            .endObject().endObject().string();
        try {
            mapperService.merge(PercolatorService.TYPE_NAME, new CompressedXContent(percolatorMapper), MapperService.MergeReason.MAPPING_UPDATE, true);
            fail("MapperParsingException expected");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Mapping definition for [query] has unsupported parameters:  [index : no]"));
        }
    }

}
