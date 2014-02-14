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

package org.elasticsearch.index.mapper.ttl;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TTLMappingTests extends ElasticsearchTestCase {
    @Test
    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl"), equalTo(null));
    }

    @Test
    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("enabled", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl").fieldType().stored(), equalTo(true));
        assertThat(doc.rootDoc().getField("_ttl").fieldType().indexed(), equalTo(true));
        assertThat(doc.rootDoc().getField("_ttl").tokenStream(docMapper.indexAnalyzer()), notNullValue());
    }

    @Test
    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(TTLFieldMapper.Defaults.ENABLED_STATE.enabled));
        assertThat(docMapper.TTLFieldMapper().fieldType().stored(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.stored()));
        assertThat(docMapper.TTLFieldMapper().fieldType().indexed(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.indexed()));
    }


    @Test
    public void testSetValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes").field("store", "no").field("index", "no")
                .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(true));
        assertThat(docMapper.TTLFieldMapper().fieldType().stored(), equalTo(false));
        assertThat(docMapper.TTLFieldMapper().fieldType().indexed(), equalTo(false));
    }

    @Test
    public void testThatEnablingTTLFieldOnMergeWorks() throws Exception {
        String mappingWithoutTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes").field("store", "no").field("index", "no")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapperWithoutTtl = MapperTestUtils.newParser().parse(mappingWithoutTtl);
        DocumentMapper mapperWithTtl = MapperTestUtils.newParser().parse(mappingWithTtl);

        DocumentMapper.MergeFlags mergeFlags = DocumentMapper.MergeFlags.mergeFlags().simulate(false);
        DocumentMapper.MergeResult mergeResult = mapperWithoutTtl.merge(mapperWithTtl, mergeFlags);

        assertThat(mergeResult.hasConflicts(), equalTo(false));
        assertThat(mapperWithoutTtl.TTLFieldMapper().enabled(), equalTo(true));
    }

    @Test
    public void testThatChangingTTLKeepsMapperEnabled() throws Exception {
        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("default", "1w")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper initialMapper = MapperTestUtils.newParser().parse(mappingWithTtl);
        DocumentMapper updatedMapper = MapperTestUtils.newParser().parse(updatedMapping);

        DocumentMapper.MergeFlags mergeFlags = DocumentMapper.MergeFlags.mergeFlags().simulate(false);
        DocumentMapper.MergeResult mergeResult = initialMapper.merge(updatedMapper, mergeFlags);

        assertThat(mergeResult.hasConflicts(), equalTo(false));
        assertThat(initialMapper.TTLFieldMapper().enabled(), equalTo(true));
    }
}