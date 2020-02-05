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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

public class GenericStoreDynamicTemplateTests extends ESSingleNodeTestCase {
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();

        MapperService mapperService = index.mapperService();

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(
            new SourceToParse("test", "1", new BytesArray(json), XContentType.JSON));
        client().admin().indices().preparePutMapping("test")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        MappedFieldType fieldType = mapperService.fullName("name");
        assertThat(fieldType.stored(), equalTo(true));

        boolean stored = false;
        for (IndexableField field : doc.getFields("age")) {
            stored |=  field.fieldType().stored();
        }
        assertTrue(stored);

        fieldType = mapperService.fullName("age");
        assertThat(fieldType.stored(), equalTo(true));
    }
}
