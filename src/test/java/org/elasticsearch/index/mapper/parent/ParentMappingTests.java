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
package org.elasticsearch.index.mapper.parent;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class ParentMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void parentNotMapped() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("_parent", "1122")
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1"));

        // no _parent mapping, used as a simple field
        assertThat(doc.parent(), nullValue());
        assertThat(doc.rootDoc().get("_parent"), nullValue());
    }

    @Test
    public void parentSetInDocNotExternally() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "p_type").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("_parent", "1122")
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1"));

        assertThat(doc.parent(), equalTo("1122"));
        assertThat(doc.rootDoc().get("_parent"), equalTo(Uid.createUid("p_type", "1122")));
    }

    @Test
    public void parentNotSetInDocSetExternally() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "p_type").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1").parent("1122"));

        assertThat(doc.rootDoc().get("_parent"), equalTo(Uid.createUid("p_type", "1122")));
    }

    @Test
    public void parentSetInDocSetExternally() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "p_type").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("_parent", "1122")
                .field("x_field", "x_value")
                .endObject()
                .bytes()).type("type").id("1").parent("1122"));

        assertThat(doc.rootDoc().get("_parent"), equalTo(Uid.createUid("p_type", "1122")));
    }
}
