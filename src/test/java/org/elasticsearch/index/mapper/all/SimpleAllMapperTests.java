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

package org.elasticsearch.index.mapper.all;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleAllMapperTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleAllMappers() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(AllTermQuery.class));
    }

    @Test
    public void testAllMappersNoBoost() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/noboost-mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(TermQuery.class));
    }

    @Test
    public void testAllMappersTermQuery() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_omit_positions_on_all.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(TermQuery.class));

    }


    @Test
    public void testSimpleAllMappersWithReparse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = MapperTestUtils.newParser().parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
    }

    @Test
    public void testSimpleAllMappersWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
    }

    @Test
    public void testSimpleAllMappersWithReparseWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = MapperTestUtils.newParser().parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStream(docMapper.mappers().indexAnalyzer())).allEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
    }
}
