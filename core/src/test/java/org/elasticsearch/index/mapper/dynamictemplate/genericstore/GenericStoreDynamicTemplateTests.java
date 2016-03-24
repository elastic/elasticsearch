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

package org.elasticsearch.index.mapper.dynamictemplate.genericstore;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class GenericStoreDynamicTemplateTests extends ESSingleNodeTestCase {
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("person").setSource(mapping).get();
        DocumentMapper docMapper = index.mapperService().documentMapper("person");
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-data.json");
        ParsedDocument parsedDoc = docMapper.parse("test", "person", "1", new BytesArray(json));
        client().admin().indices().preparePutMapping("test").setType("person").setSource(parsedDoc.dynamicMappingsUpdate().toString()).get();
        docMapper = index.mapperService().documentMapper("person");
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        FieldMapper fieldMapper = docMapper.mappers().getMapper("name");
        assertThat(fieldMapper.fieldType().stored(), equalTo(true));

        f = doc.getField("age");
        assertThat(f.name(), equalTo("age"));
        assertThat(f.fieldType().stored(), equalTo(true));

        fieldMapper = docMapper.mappers().getMapper("age");
        assertThat(fieldMapper.fieldType().stored(), equalTo(true));
    }
}
