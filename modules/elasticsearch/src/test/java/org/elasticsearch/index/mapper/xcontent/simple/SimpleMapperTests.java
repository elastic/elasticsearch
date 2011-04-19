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

package org.elasticsearch.index.mapper.xcontent.simple;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.xcontent.MapperTests;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapperParser;
import org.testng.annotations.Test;

import static org.apache.lucene.document.Field.Store.*;
import static org.elasticsearch.common.io.Streams.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public class SimpleMapperTests {

    @Test public void testSimpleMapper() throws Exception {
        XContentDocumentMapperParser mapperParser = MapperTests.newParser();
        XContentDocumentMapper docMapper = doc("test",
                rootObject("person")
                        .add(object("name").add(stringField("first").store(YES).index(Field.Index.NO)))
        ).sourceField(source()).build(mapperParser);

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test1.json");
        Document doc = docMapper.parse("person", "1", json).doc();

        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
        assertThat(docMapper.mappers().name("first").mapper().names().fullName(), equalTo("name.first"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
        doc = docMapper.parse(json).doc();
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test-mapping.json");
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        XContentDocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test1.json");
        Document doc = builtDocMapper.parse(json).doc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test-mapping.json");
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test1.json");
        Document doc = docMapper.parse(json).doc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test-mapping.json");
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test1-notype-noid.json");
        Document doc = docMapper.parse("person", "1", json).doc();
        assertThat(doc.get(docMapper.uidMapper().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/simple/test-mapping.json");
        XContentDocumentMapper docMapper = MapperTests.newParser().parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        XContentDocumentMapper builtDocMapper = MapperTests.newParser().parse(builtMapping);
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }
}
