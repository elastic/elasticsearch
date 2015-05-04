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

package org.elasticsearch.index.mapper.multifield.merge;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class JavaMultiFieldMergeTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testMergeMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), nullValue());

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping2.json");
        DocumentMapper docMapper2 = parser.parse(mapping);

        MergeResult mergeResult = docMapper.merge(docMapper2.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapper.merge(docMapper2.mapping(), false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());

        json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-data.json"));
        doc = docMapper.parse(json).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping3.json");
        DocumentMapper docMapper3 = parser.parse(mapping);

        mergeResult = docMapper.merge(docMapper3.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapper.merge(docMapper3.mapping(), false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping4.json");
        DocumentMapper docMapper4 = parser.parse(mapping);


        mergeResult = docMapper.merge(docMapper4.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapper.merge(docMapper4.mapping(), false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), notNullValue());
    }

    @Test
    public void testUpgradeFromMultiFieldTypeToMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), nullValue());

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-data.json"));
        Document doc = docMapper.parse(json).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade1.json");
        DocumentMapper docMapper2 = parser.parse(mapping);

        MergeResult mergeResult = docMapper.merge(docMapper2.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapper.merge(docMapper2.mapping(), false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());

        json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-data.json"));
        doc = docMapper.parse(json).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade2.json");
        DocumentMapper docMapper3 = parser.parse(mapping);

        mergeResult = docMapper.merge(docMapper3.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(false));

        docMapper.merge(docMapper3.mapping(), false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade3.json");
        DocumentMapper docMapper4 = parser.parse(mapping);
        mergeResult = docMapper.merge(docMapper4.mapping(), true);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(true));
        assertThat(mergeResult.buildConflicts()[0], equalTo("mapper [name] has different index values"));
        assertThat(mergeResult.buildConflicts()[1], equalTo("mapper [name] has different store values"));

        mergeResult = docMapper.merge(docMapper4.mapping(), false);
        assertThat(Arrays.toString(mergeResult.buildConflicts()), mergeResult.hasConflicts(), equalTo(true));

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(mergeResult.buildConflicts()[0], equalTo("mapper [name] has different index values"));
        assertThat(mergeResult.buildConflicts()[1], equalTo("mapper [name] has different store values"));

        // There are conflicts, but the `name.not_indexed3` has been added, b/c that field has no conflicts
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), notNullValue());
    }
}
