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

package org.elasticsearch.index.mapper.boost;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.BoostFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class BoostMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testDefaultMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("_boost", 2.0f)
                .field("field", "a")
                .field("field", "b")
                .endObject().bytes());

        // one fo the same named field will have the proper boost, the others will have 1
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertThat(fields[0].boost(), equalTo(2.0f));
        assertThat(fields[1].boost(), equalTo(1.0f));
    }

    @Test
    public void testCustomName() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_boost").field("name", "custom_boost").endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "a")
                .field("_boost", 2.0f)
                .endObject().bytes());
        assertThat(doc.rootDoc().getField("field").boost(), equalTo(1.0f));

        doc = mapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "a")
                .field("custom_boost", 2.0f)
                .endObject().bytes());
        assertThat(doc.rootDoc().getField("field").boost(), equalTo(2.0f));
    }

    @Test
    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertThat(docMapper.boostFieldMapper().fieldType().stored(), equalTo(BoostFieldMapper.Defaults.FIELD_TYPE.stored()));
        assertThat(docMapper.boostFieldMapper().fieldType().indexed(), equalTo(BoostFieldMapper.Defaults.FIELD_TYPE.indexed()));
    }

    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_boost")
                .field("store", "yes").field("index", "not_analyzed")
                .endObject()
                .endObject().endObject().string();
        IndexService indexServices = createIndex("test");
        DocumentMapper docMapper = indexServices.mapperService().documentMapperParser().parse("type", mapping);
        assertThat(docMapper.boostFieldMapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.boostFieldMapper().fieldType().indexed(), equalTo(true));
        docMapper.refreshSource();
        docMapper = indexServices.mapperService().documentMapperParser().parse("type", docMapper.mappingSource().string());
        assertThat(docMapper.boostFieldMapper().fieldType().stored(), equalTo(true));
        assertThat(docMapper.boostFieldMapper().fieldType().indexed(), equalTo(true));
    }
}
