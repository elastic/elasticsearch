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
package org.elasticsearch.index.mapper.completion;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.suggest.xdocument.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.OldCompletionFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.Version.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

public class CompletionFieldMapperTests extends ESSingleNodeTestCase {
    private final Version PRE2X_VERSION = VersionUtils.randomVersionBetween(getRandom(), Version.V_1_0_0, Version.V_1_7_0);

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        MappedFieldType completionFieldType = fieldMapper.fieldType();

        NamedAnalyzer indexAnalyzer = completionFieldType.indexAnalyzer();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.class));
        org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer analyzer = (org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));

        NamedAnalyzer searchAnalyzer = completionFieldType.searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("simple"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.class));
        analyzer = (org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));
    }

    @Test
    public void testCompletionAnalyzerSettings() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("index_analyzer", "simple")
                .field("search_analyzer", "standard")
                .field("preserve_separators", false)
                .field("preserve_position_increments", true)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        MappedFieldType completionFieldType = fieldMapper.fieldType();

        NamedAnalyzer indexAnalyzer = completionFieldType.indexAnalyzer();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.class));
        org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer analyzer = (org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

        NamedAnalyzer searchAnalyzer = completionFieldType.searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("standard"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer.class));
        analyzer = (org.apache.lucene.search.suggest.xdocument.CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

    }

    @Test
    public void testThatSerializationIncludesAllElements() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("index_analyzer", "simple")
                .field("search_analyzer", "standard")
                .field("preserve_separators", false)
                .field("preserve_position_increments", true)
                .field("max_input_length", 14)
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        completionFieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap = JsonXContent.jsonXContent.createParser(builder.bytes()).map();
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("completion");
        assertThat(configMap.get("index_analyzer").toString(), is("simple"));
        assertThat(configMap.get("search_analyzer").toString(), is("standard"));
        assertThat(Boolean.valueOf(configMap.get("preserve_separators").toString()), is(false));
        assertThat(Boolean.valueOf(configMap.get("preserve_position_increments").toString()), is(true));
        assertThat(Integer.valueOf(configMap.get("max_input_length").toString()), is(14));
    }

    @Test
    public void testFieldSerializationMinimal() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("completion", "suggestion")
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 1);
    }

    @Test
    public void testCreatingAndIndexingSuggestFieldForOlderIndices() throws Exception {
        // creating completion field for pre 2.0 indices, should create old completion fields
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();
        for (Version version : Arrays.asList(V_1_7_0, V_1_1_0, randomVersionBetween(random(), V_1_1_0, V_1_7_0))) {
            DocumentMapper defaultMapper = createIndex("test", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version.id).build())
                    .mapperService().documentMapperParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
            assertTrue(fieldMapper instanceof OldCompletionFieldMapper);
            MappedFieldType completionFieldType = fieldMapper.fieldType();
            ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", "suggestion")
                    .endObject()
                    .bytes());
            IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
            assertThat(fields.length, equalTo(1));
            assertFalse(fields[0] instanceof org.apache.lucene.search.suggest.xdocument.SuggestField);
            assertAcked(client().admin().indices().prepareDelete("test").execute().get());
        }

        // creating completion field for pre 2.0 indices with explicit force_new should create new completion fields
        mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("force_new", true)
                .endObject().endObject()
                .endObject().endObject().string();
        for (Version version : Arrays.asList(V_1_7_0, V_1_1_0, randomVersionBetween(random(), V_1_1_1, V_1_6_1))) {
            Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version.id).build();
            DocumentMapper defaultMapper = createIndex("test", settings)
                    .mapperService().documentMapperParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
            assertTrue(fieldMapper instanceof CompletionFieldMapper);
            MappedFieldType completionFieldType = fieldMapper.fieldType();
            ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", "suggestion")
                    .endObject()
                    .bytes());
            IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
            assertThat(fields.length, equalTo(1));
            assertTrue(fields[0] instanceof org.apache.lucene.search.suggest.xdocument.SuggestField);
            assertAcked(client().admin().indices().prepareDelete("test").execute().get());

            // when force_new is specified, ensure it gets serialized
            XContentBuilder builder = JsonXContent.contentBuilder().startObject();
            fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
            builder.close();
            Map<String, Object> serializedMap;
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes())) {
                serializedMap = parser.map();
                serializedMap = ((Map<String, Object>) serializedMap.get("completion"));
            }
            assertThat(((boolean)serializedMap.get("force_new")), equalTo(true));
        }
    }

    @Test
    public void testFieldSerializationMinimalMultiValued() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .array("completion", "suggestion1", "suggestion2")
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 2);
    }

    @Test
    public void testFieldSerializationWithWeight() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("completion")
                .field("input", "suggestion")
                .field("weight", 2)
                .endObject()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 1);
    }

    @Test
    public void testFieldSerializationMultiValueWithWeight() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("completion")
                .array("input", "suggestion1", "suggestion2", "suggestion3")
                .field("weight", 2)
                .endObject()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 3);
    }

    @Test
    public void testFieldSerializationFullOption() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion1")
                .field("weight", 3)
                .endObject()
                .startObject()
                .field("input", "suggestion2")
                .field("weight", 4)
                .endObject()
                .startObject()
                .field("input", "suggestion3")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 3);
    }

    @Test
    public void testFieldSerializationFullOptionMixed() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion1", "suggestion2")
                .field("weight", 3)
                .endObject()
                .startObject()
                .field("input", "suggestion3")
                .field("weight", 4)
                .endObject()
                .startObject()
                .field("input", "suggestion4", "suggestion5", "suggestion6")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 6);
    }

    @Test
    public void testFieldSerializationFullOptionWithMultiValue() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion1", "suggestion2")
                .field("weight", 3)
                .endObject()
                .startObject()
                .array("input", "suggestion3", "suggestion4")
                .field("weight", 4)
                .endObject()
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertSuggestFields(fields, 7);
    }

    @Test
    public void testNonContextEnabledFieldWithContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("field1")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        try {
            defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("field1")
                    .field("input", "suggestion1")
                    .startObject("contexts")
                    .field("ctx", "ctx2")
                    .endObject()
                    .field("weight", 3)
                    .endObject()
                    .endObject()
                    .bytes());
            fail("Supplying contexts to a non context-enabled field should error");
        } catch (MapperParsingException e) {
            assertThat(e.getRootCause().getMessage(), containsString("field1"));
        }
    }

    @Test
    public void testContextEnabledFieldSerializationWithNoContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                    .startObject()
                        .field("name", "ctx")
                        .field("type", "category")
                    .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .array("input", "suggestion1", "suggestion2")
                .field("weight", 3)
                .endObject()
                .startObject()
                .array("input", "suggestion3", "suggestion4")
                .field("weight", 4)
                .endObject()
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertContextSuggestFields(fields, 7);
    }

    @Test
    public void testFieldSerializationWithSimpleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                    .startObject()
                        .field("name", "ctx")
                        .field("type", "category")
                    .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                    .field("ctx", "ctx1")
                .endObject()
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertContextSuggestFields(fields, 3);
    }

    @Test
    public void testFieldSerializationWithSimpleContextsList() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "category")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .startObject("contexts")
                .array("ctx", "ctx1", "ctx2", "ctx3")
                .endObject()
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertContextSuggestFields(fields, 3);
    }

    @Test
    public void testMultipleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                    .startObject()
                        .field("name", "ctx")
                        .field("type", "category")
                    .endObject()
                    .startObject()
                        .field("name", "type")
                        .field("type", "category")
                    .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .startObject("contexts")
                .array("ctx", "ctx1", "ctx2", "ctx3")
                .array("type", "typr3", "ftg")
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        String string = builder.string();
        ParsedDocument parsedDocument = defaultMapper.parse("type1", "1", builder.bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.names().indexName());
        assertContextSuggestFields(fields, 3);
    }

    @Test
    public void testFieldValueValidation() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
        charsRefBuilder.append("sugg");
        charsRefBuilder.setCharAt(2, '\u001F');
        try {
            defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", charsRefBuilder.get().toString())
                    .endObject()
                    .bytes());
            fail("No error indexing value with reserved character [0x1F]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1f]"));
        }

        charsRefBuilder.setCharAt(2, '\u0000');
        try {
            defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", charsRefBuilder.get().toString())
                    .endObject()
                    .bytes());
            fail("No error indexing value with reserved character [0x0]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x0]"));
        }

        charsRefBuilder.setCharAt(2, '\u001E');
        try {
            defaultMapper.parse("type1", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", charsRefBuilder.get().toString())
                    .endObject()
                    .bytes());
            fail("No error indexing value with reserved character [0x1E]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1e]"));
        }
    }

    @Test
    public void testPrefixQueryType() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType().prefixQuery(new BytesRef("co"));
        assertThat(prefixQuery, instanceOf(PrefixCompletionQuery.class));
    }

    @Test
    public void testFuzzyQueryType() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType().fuzzyQuery("co",
                Fuzziness.fromEdits(FuzzyCompletionQuery.DEFAULT_MAX_EDITS), FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX,
                FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH, Operations.DEFAULT_MAX_DETERMINIZED_STATES,
                FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS, FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE);
        assertThat(prefixQuery, instanceOf(FuzzyCompletionQuery.class));
    }

    @Test
    public void testRegexQueryType() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType()
                .regexpQuery(new BytesRef("co"), RegExp.ALL, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        assertThat(prefixQuery, instanceOf(RegexCompletionQuery.class));
    }

    private static void assertContextSuggestFields(IndexableField[] fields, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (field instanceof ContextSuggestField) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }

    private static void assertSuggestFields(IndexableField[] fields, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (field instanceof org.apache.lucene.search.suggest.xdocument.SuggestField) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }
}
