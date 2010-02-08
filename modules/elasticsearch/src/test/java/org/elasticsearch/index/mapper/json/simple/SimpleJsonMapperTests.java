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

package org.elasticsearch.index.mapper.json.simple;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.json.JsonDocumentMapper;
import org.elasticsearch.index.mapper.json.JsonDocumentMapperParser;
import org.testng.annotations.Test;

import static org.apache.lucene.document.Field.Store.*;
import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.util.io.Streams.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public class SimpleJsonMapperTests {

    @Test public void testSimpleMapper() throws Exception {
        JsonDocumentMapper docMapper = doc(
                object("person")
                        .add(object("name").add(stringField("first").store(YES).index(Field.Index.NO)))
        ).sourceField(source("_source").compressionThreshold(0)).build();

        String json = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test1.json");
        Document doc = docMapper.parse("person", "1", json).doc();

        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().indexName()), equalTo("shay"));
        assertThat(docMapper.mappers().name("first").mapper().fullName(), equalTo("name.first"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
        doc = docMapper.parse(json).doc();
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test-mapping.json");
        JsonDocumentMapper docMapper = (JsonDocumentMapper) new JsonDocumentMapperParser(new AnalysisService(new Index("test"))).parse(mapping);
        String json = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test1.json");
        Document doc = docMapper.parse(json).doc();
        assertThat(doc.get(docMapper.uidMapper().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().indexName()), equalTo("shay"));
        assertThat(doc.getFields(docMapper.idMapper().indexName()).length, equalTo(1));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testSimpleParserMappingWithNoType() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test-mapping-notype.json");
        JsonDocumentMapper docMapper = (JsonDocumentMapper) new JsonDocumentMapperParser(new AnalysisService(new Index("test"))).parse("person", mapping);
        String json = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test1.json");
        Document doc = docMapper.parse(json).doc();
        assertThat(doc.get(docMapper.uidMapper().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().indexName()), equalTo("shay"));
        assertThat(doc.getFields(docMapper.idMapper().indexName()).length, equalTo(1));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test-mapping.json");
        JsonDocumentMapper docMapper = (JsonDocumentMapper) new JsonDocumentMapperParser(new AnalysisService(new Index("test"))).parse(mapping);
        String json = copyToStringFromClasspath("/org/elasticsearch/index/mapper/json/simple/test1-notype-noid.json");
        Document doc = docMapper.parse("person", "1", json).doc();
        assertThat(doc.get(docMapper.uidMapper().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat((double) doc.getBoost(), closeTo(3.7, 0.01));
        assertThat(doc.get(docMapper.mappers().name("first").mapper().indexName()), equalTo("shay"));
        assertThat(doc.getFields(docMapper.idMapper().indexName()).length, equalTo(1));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }
}
