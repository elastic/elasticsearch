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

package org.elasticsearch.index.mapper.id;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTests;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 */
@Test
public class IdMappingTests {

    @Test public void simpleIdTests() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .copiedBytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), nullValue());

        try {
            docMapper.parse("type", null, XContentFactory.jsonBuilder()
                    .startObject()
                    .endObject()
                    .copiedBytes());
            assert false;
        } catch (MapperParsingException e) {
        }

        doc = docMapper.parse("type", null, XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", 1)
                .endObject()
                .copiedBytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), nullValue());
    }

    @Test public void testIdIndexed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_id").field("index", "not_analyzed").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .endObject()
                .copiedBytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), notNullValue());

        doc = docMapper.parse("type", null, XContentFactory.jsonBuilder()
                .startObject()
                .field("_id", 1)
                .endObject()
                .copiedBytes());

        assertThat(doc.rootDoc().get(UidFieldMapper.NAME), notNullValue());
        assertThat(doc.rootDoc().get(IdFieldMapper.NAME), notNullValue());
    }
}
