/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent.size;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.xcontent.MapperTests;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Test
public class SizeMappingTests {

    @Test public void testSizeEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", true).endObject()
                .endObject().endObject().string();
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        byte[] source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .copiedBytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.doc().getFieldable("_size").isStored(), equalTo(false));
        assertThat(doc.doc().getFieldable("_size").tokenStreamValue(), notNullValue());
    }

    @Test public void testSizeEnabledAndStored() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", true).field("store", "yes").endObject()
                .endObject().endObject().string();
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        byte[] source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .copiedBytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.doc().getFieldable("_size").isStored(), equalTo(true));
        assertThat(doc.doc().getFieldable("_size").tokenStreamValue(), notNullValue());
    }

    @Test public void testSizeDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_size").field("enabled", false).endObject()
                .endObject().endObject().string();
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        byte[] source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .copiedBytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.doc().getFieldable("_size"), nullValue());
    }

    @Test public void testSizeNotSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        byte[] source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .copiedBytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1"));

        assertThat(doc.doc().getFieldable("_size"), nullValue());
    }
}