/* exception when already expired

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

package org.elasticsearch.test.unit.index.mapper.ttl;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TTLMappingTests {
    @Test
    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getFieldable("_ttl"), equalTo(null));
    }

    @Test
    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("enabled", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getFieldable("_ttl").isStored(), equalTo(true));
        assertThat(doc.rootDoc().getFieldable("_ttl").isIndexed(), equalTo(true));
        assertThat(doc.rootDoc().getFieldable("_ttl").tokenStreamValue(), notNullValue());
    }

    @Test
    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(TTLFieldMapper.Defaults.ENABLED));
        assertThat(docMapper.TTLFieldMapper().store(), equalTo(TTLFieldMapper.Defaults.STORE));
        assertThat(docMapper.TTLFieldMapper().index(), equalTo(TTLFieldMapper.Defaults.INDEX));
    }


    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes").field("store", "no").field("index", "no")
                .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(true));
        assertThat(docMapper.TTLFieldMapper().store(), equalTo(Field.Store.NO));
        assertThat(docMapper.TTLFieldMapper().index(), equalTo(Field.Index.NO));
    }
}