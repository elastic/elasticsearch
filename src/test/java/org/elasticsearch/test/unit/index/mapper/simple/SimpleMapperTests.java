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

package org.elasticsearch.test.unit.index.mapper.simple;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.apache.lucene.document.Field.Store.YES;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleMapperTests {

    @Test
    public void testSimpleMapper() throws Exception {
        DocumentMapperParser mapperParser = MapperTests.newParser();
        DocumentMapper docMapper = doc("test",
                rootObject("person")
                        .add(object("name").add(stringField("first").store(YES).index(Field.Index.NO)))
        ).build(mapperParser);

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse("person", "1", json).rootDoc();

        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
        assertThat(docMapper.mappers().name("first").mapper().names().fullName(), equalTo("name.first"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
        doc = docMapper.parse(json).rootDoc();
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        DocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test1.json"));
        Document doc = builtDocMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test1-notype-noid.json"));
        Document doc = docMapper.parse("person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/test/unit/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }
}
