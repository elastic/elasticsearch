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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class GenericStoreDynamicTemplateTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-data.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        FieldMappers fieldMappers = docMapper.mappers().fullName("name");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().fieldType().stored(), equalTo(true));

        f = doc.getField("age");
        assertThat(f.name(), equalTo("age"));
        assertThat(f.fieldType().stored(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("age");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
        assertThat(fieldMappers.mapper().fieldType().stored(), equalTo(true));
    }
}
