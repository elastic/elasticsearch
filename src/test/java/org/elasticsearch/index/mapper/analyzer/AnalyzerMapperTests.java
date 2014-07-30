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

package org.elasticsearch.index.mapper.analyzer;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class AnalyzerMapperTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testAnalyzerMapping() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_analyzer").field("path", "field_analyzer").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .startObject("field2").field("type", "string").field("analyzer", "simple").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = parser.parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        FieldNameAnalyzer analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));

        // check that it serializes and de-serializes correctly

        DocumentMapper reparsedMapper = parser.parse(documentMapper.mappingSource().string());

        doc = reparsedMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));
    }


    @Test
    public void testAnalyzerMappingExplicit() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_analyzer").field("path", "field_analyzer").endObject()
                .startObject("properties")
                .startObject("field_analyzer").field("type", "string").endObject()
                .startObject("field1").field("type", "string").endObject()
                .startObject("field2").field("type", "string").field("analyzer", "simple").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = parser.parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        FieldNameAnalyzer analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));

        // check that it serializes and de-serializes correctly

        DocumentMapper reparsedMapper = parser.parse(documentMapper.mappingSource().string());

        doc = reparsedMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));
    }

    @Test
    public void testAnalyzerMappingNotIndexedNorStored() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_analyzer").field("path", "field_analyzer").endObject()
                .startObject("properties")
                .startObject("field_analyzer").field("type", "string").field("index", "no").field("store", "no").endObject()
                .startObject("field1").field("type", "string").endObject()
                .startObject("field2").field("type", "string").field("analyzer", "simple").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = parser.parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        FieldNameAnalyzer analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));

        // check that it serializes and de-serializes correctly

        DocumentMapper reparsedMapper = parser.parse(documentMapper.mappingSource().string());

        doc = reparsedMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field_analyzer", "whitespace")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().bytes());

        analyzer = (FieldNameAnalyzer) doc.analyzer();
        assertThat(((NamedAnalyzer) analyzer.defaultAnalyzer()).name(), equalTo("whitespace"));
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field1")), nullValue());
        assertThat(((NamedAnalyzer) analyzer.analyzers().get("field2")).name(), equalTo("simple"));
    }
}
