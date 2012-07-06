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

package org.elasticsearch.test.unit.index.mapper.all;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class SimpleAllMapperTests {

    @Test
    public void testSimpleAllMappers() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getFieldable("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStreamValue()).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
    }

    @Test
    public void testSimpleAllMappersWithReparse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        DocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getFieldable("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStreamValue()).allEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
    }

    @Test
    public void testSimpleAllMappersWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getFieldable("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStreamValue()).allEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
    }

    @Test
    public void testSimpleAllMappersWithReparseWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        System.out.println(builtMapping);
        // reparse it
        DocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getFieldable("_all");
        AllEntries allEntries = ((AllTokenStream) field.tokenStreamValue()).allEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
    }
}
