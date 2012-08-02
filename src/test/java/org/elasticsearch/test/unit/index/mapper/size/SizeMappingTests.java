/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.mapper.size;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Test
public class SizeMappingTests {

    @Test
    public void testSizeEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", true).endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getFieldable("_size").isStored(), equalTo(false));
        assertThat(doc.rootDoc().getFieldable("_size").tokenStreamValue(), notNullValue());
    }

    @Test
    public void testSizeEnabledAndStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", true).field("store", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getFieldable("_size").isStored(), equalTo(true));
        assertThat(doc.rootDoc().getFieldable("_size").tokenStreamValue(), notNullValue());
    }

    @Test
    public void testSizeDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getFieldable("_size"), nullValue());
    }

    @Test
    public void testSizeNotSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.rootDoc().getFieldable("_size"), nullValue());
    }
}