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

package org.elasticsearch.index.mapper.multifield;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class MultiFieldTests extends ElasticsearchTestCase {

    @Test
    public void testMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/test-mapping.json");
        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(false));

        f = doc.getField("object1.multi1");
        assertThat(f.name(), equalTo("object1.multi1"));

        f = doc.getField("object1.multi1.string");
        assertThat(f.name(), equalTo("object1.multi1.string"));
        assertThat(f.stringValue(), equalTo("2010-01-01"));
    }

    @Test
    public void testBuildThenParse() throws Exception {
        DocumentMapperParser mapperParser = MapperTestUtils.newParser();

        DocumentMapper builderDocMapper = doc("test", rootObject("person").add(
                multiField("name")
                        .add(stringField("name").store(true))
                        .add(stringField("indexed").index(true).tokenized(true))
                        .add(stringField("not_indexed").index(false).store(true))
        )).build(mapperParser);
        builderDocMapper.refreshSource();

        String builtMapping = builderDocMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        DocumentMapper docMapper = mapperParser.parse(builtMapping);


        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.indexed");
        assertThat(f.name(), equalTo("name.indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(false));
        assertThat(f.fieldType().indexed(), equalTo(true));

        f = doc.getField("name.not_indexed");
        assertThat(f.name(), equalTo("name.not_indexed"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));
        assertThat(f.fieldType().indexed(), equalTo(false));
    }
}
