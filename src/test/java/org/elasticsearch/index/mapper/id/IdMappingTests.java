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

package org.elasticsearch.index.mapper.id;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
public class IdMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void simpleIdTests() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), nullValue());

        try {
            docMapper.parse("type", null, XContentFactory.jsonBuilder()
                    .startObject()
                    .endObject()
                    .bytes());
            fail();
        } catch (MapperParsingException e) {
        }

        doc = docMapper.parse("type", null, XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", 1)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), nullValue());
    }

    @Test
    public void testIdIndexed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").field("index", "not_analyzed").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), notNullValue());

        doc = docMapper.parse("type", null, XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", 1)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), notNullValue());
    }

    @Test
    public void testIdPath() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").field("path", "my_path").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        // serialize the id mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = docMapper.idFieldMapper().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String serialized_id_mapping = builder.string();

        String expected_id_mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("_id").field("path", "my_path").endObject()
                .endObject().string();

        assertThat(serialized_id_mapping, equalTo(expected_id_mapping));
    }
}
