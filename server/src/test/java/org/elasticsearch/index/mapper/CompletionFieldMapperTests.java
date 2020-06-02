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

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.suggest.document.CompletionAnalyzer;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.RegexCompletionQuery;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.CombinableMatcher;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CompletionFieldMapperTests extends FieldMapperTestCase<CompletionFieldMapper.Builder> {

    @Before
    public void addModifiers() {
        addBooleanModifier("preserve_separators", false, CompletionFieldMapper.Builder::preserveSeparators);
        addBooleanModifier("preserve_position_increments", false, CompletionFieldMapper.Builder::preservePositionIncrements);
        addModifier("context_mappings", false, (a, b) -> {
            ContextMappings contextMappings = new ContextMappings(Arrays.asList(ContextBuilder.category("foo").build(),
                ContextBuilder.geo("geo").build()));
            a.contextMappings(contextMappings);
        });
    }

    @Override
    protected CompletionFieldMapper.Builder newBuilder() {
        return new CompletionFieldMapper.Builder("completion");
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));
        MappedFieldType completionFieldType = ((CompletionFieldMapper) fieldMapper).fieldType();

        NamedAnalyzer indexAnalyzer = completionFieldType.indexAnalyzer();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        CompletionAnalyzer analyzer = (CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));

        NamedAnalyzer searchAnalyzer = completionFieldType.searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("simple"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        analyzer = (CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));
    }

    public void testCompletionAnalyzerSettings() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("analyzer", "simple")
                .field("search_analyzer", "standard")
                .field("preserve_separators", false)
                .field("preserve_position_increments", true)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));
        MappedFieldType completionFieldType = ((CompletionFieldMapper) fieldMapper).fieldType();

        NamedAnalyzer indexAnalyzer = completionFieldType.indexAnalyzer();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        CompletionAnalyzer analyzer = (CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

        NamedAnalyzer searchAnalyzer = completionFieldType.searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("standard"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        analyzer = (CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

    }

    public void testTypeParsing() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .field("analyzer", "simple")
                .field("search_analyzer", "standard")
                .field("preserve_separators", false)
                .field("preserve_position_increments", true)
                .field("max_input_length", 14)
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        XContentBuilder builder = jsonBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();
        Map<String, Object> serializedMap = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder)).map();
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("completion");
        assertThat(configMap.get("analyzer").toString(), is("simple"));
        assertThat(configMap.get("search_analyzer").toString(), is("standard"));
        assertThat(Boolean.valueOf(configMap.get("preserve_separators").toString()), is(false));
        assertThat(Boolean.valueOf(configMap.get("preserve_position_increments").toString()), is(true));
        assertThat(Integer.valueOf(configMap.get("max_input_length").toString()), is(14));
    }

    public void testParsingMinimal() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("completion", "suggestion")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertSuggestFields(fields, 1);
    }

    public void testParsingFailure() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("completion", 1.0)
                            .endObject()),
                XContentType.JSON)));
        assertEquals("failed to parse [completion]: expected text or object, but got VALUE_NUMBER", e.getCause().getMessage());
    }

    public void testKeywordWithSubCompletionAndContext() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
            .startObject("keywordfield")
                .field("type", "keyword")
                .startObject("fields")
                .startObject("subsuggest")
                    .field("type", "completion")
                    .startArray("contexts")
                        .startObject()
                            .field("name","place_type")
                            .field("type","category")
                            .field("path","cat")
                        .endObject()
                    .endArray()
                .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .array("keywordfield", "key1", "key2", "key3")
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();

        assertThat(indexableFields.getFields("keywordfield"), arrayContainingInAnyOrder(
            keywordField("key1"),
            sortedSetDocValuesField("key1"),
            keywordField("key2"),
            sortedSetDocValuesField("key2"),
            keywordField("key3"),
            sortedSetDocValuesField("key3")
        ));
        assertThat(indexableFields.getFields("keywordfield.subsuggest"), arrayContainingInAnyOrder(
            contextSuggestField("key1"),
            contextSuggestField("key2"),
            contextSuggestField("key3")
        ));
    }

    public void testCompletionWithContextAndSubCompletion() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
            .startObject("suggest")
                .field("type", "completion")
                .startArray("contexts")
                    .startObject()
                        .field("name","place_type")
                        .field("type","category")
                        .field("path","cat")
                    .endObject()
                .endArray()
                .startObject("fields")
                .startObject("subsuggest")
                    .field("type", "completion")
                    .startArray("contexts")
                        .startObject()
                            .field("name","place_type")
                            .field("type","category")
                            .field("path","cat")
                        .endObject()
                    .endArray()
                .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("suggest")
                            .array("input","timmy","starbucks")
                            .startObject("contexts")
                                .array("place_type","cafe","food")
                            .endObject()
                            .field("weight", 3)
                        .endObject()
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("suggest"), arrayContainingInAnyOrder(
            contextSuggestField("timmy"),
            contextSuggestField("starbucks")
        ));
        assertThat(indexableFields.getFields("suggest.subsuggest"), arrayContainingInAnyOrder(
            contextSuggestField("timmy"),
            contextSuggestField("starbucks")
        ));
        //unable to assert about context, covered in a REST test
    }

    public void testCompletionWithContextAndSubCompletionIndexByPath() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
            .startObject("suggest")
                .field("type", "completion")
                .startArray("contexts")
                    .startObject()
                        .field("name","place_type")
                        .field("type","category")
                        .field("path","cat")
                    .endObject()
                .endArray()
                .startObject("fields")
                .startObject("subsuggest")
                    .field("type", "completion")
                    .startArray("contexts")
                        .startObject()
                            .field("name","place_type")
                            .field("type","category")
                            .field("path","cat")
                        .endObject()
                    .endArray()
                .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .array("suggest", "timmy","starbucks")
                    .array("cat","cafe","food")
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("suggest"), arrayContainingInAnyOrder(
            contextSuggestField("timmy"),
            contextSuggestField("starbucks")
        ));
        assertThat(indexableFields.getFields("suggest.subsuggest"), arrayContainingInAnyOrder(
            contextSuggestField("timmy"),
            contextSuggestField("starbucks")
        ));
        //unable to assert about context, covered in a REST test
    }


    public void testKeywordWithSubCompletionAndStringInsert() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("geofield")
            .field("type", "geo_point")
            .startObject("fields")
            .startObject("analyzed")
            .field("type", "completion")
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("geofield", "drm3btev3e86")//"41.12,-71.34"
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("geofield"), arrayWithSize(2));
        assertThat(indexableFields.getFields("geofield.analyzed"), arrayContainingInAnyOrder(
            suggestField("drm3btev3e86")
        ));
        //unable to assert about geofield content, covered in a REST test
    }

    public void testCompletionTypeWithSubCompletionFieldAndStringInsert() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("suggest")
                .field("type", "completion")
                .startObject("fields")
                    .startObject("subsuggest")
                    .field("type", "completion")
                    .endObject()
                .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("suggest", "suggestion")
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("suggest"), arrayContainingInAnyOrder(
            suggestField("suggestion")
        ));
        assertThat(indexableFields.getFields("suggest.subsuggest"), arrayContainingInAnyOrder(
            suggestField("suggestion")
        ));
    }

    public void testCompletionTypeWithSubCompletionFieldAndObjectInsert() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .startObject("fields")
            .startObject("analyzed")
            .field("type", "completion")
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("completion")
                    .array("input","New York", "NY")
                    .field("weight",34)
                    .endObject()
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("completion"), arrayContainingInAnyOrder(
            suggestField("New York"),
            suggestField("NY")
        ));
        assertThat(indexableFields.getFields("completion.analyzed"), arrayContainingInAnyOrder(
            suggestField("New York"),
            suggestField("NY")
        ));
        //unable to assert about weight, covered in a REST test
    }

    public void testCompletionTypeWithSubKeywordFieldAndObjectInsert() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .startObject("fields")
            .startObject("analyzed")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("completion")
                        .array("input","New York", "NY")
                        .field("weight",34)
                    .endObject()
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("completion"), arrayContainingInAnyOrder(
            suggestField("New York"),
            suggestField("NY")
        ));
        assertThat(indexableFields.getFields("completion.analyzed"), arrayContainingInAnyOrder(
            keywordField("New York"),
            sortedSetDocValuesField("New York"),
            keywordField("NY"),
            sortedSetDocValuesField("NY")
        ));
        //unable to assert about weight, covered in a REST test
    }

    public void testCompletionTypeWithSubKeywordFieldAndStringInsert() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .startObject("fields")
            .startObject("analyzed")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject()
        );

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("completion", "suggestion")
                    .endObject()),
            XContentType.JSON));

        ParseContext.Document indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("completion"), arrayContainingInAnyOrder(
            suggestField("suggestion")
        ));
        assertThat(indexableFields.getFields("completion.analyzed"), arrayContainingInAnyOrder(
            keywordField("suggestion"),
            sortedSetDocValuesField("suggestion")
        ));
    }

    public void testParsingMultiValued() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .array("completion", "suggestion1", "suggestion2")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertThat(fields, arrayContainingInAnyOrder(
            suggestField("suggestion1"),
            suggestField("suggestion2")
        ));
    }

    public void testParsingWithWeight() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("completion")
                        .field("input", "suggestion")
                        .field("weight", 2)
                        .endObject()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertThat(fields, arrayContainingInAnyOrder(
            suggestField("suggestion")
        ));
    }

    public void testParsingMultiValueWithWeight() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("completion")
                        .array("input", "suggestion1", "suggestion2", "suggestion3")
                        .field("weight", 2)
                        .endObject()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertThat(fields, arrayContainingInAnyOrder(
            suggestField("suggestion1"),
            suggestField("suggestion2"),
            suggestField("suggestion3")
        ));
    }

    public void testParsingWithGeoFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("completion")
                        .field("type", "completion")
                        .startObject("contexts")
                            .field("name", "location")
                            .field("type", "geo")
                            .field("path", "alias")
                        .endObject()
                    .endObject()
                    .startObject("birth-place")
                        .field("type", "geo_point")
                    .endObject()
                    .startObject("alias")
                        .field("type", "alias")
                        .field("path", "birth-place")
                    .endObject()
                .endObject()
            .endObject()
        .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("completion");

        ParsedDocument parsedDocument = mapperService.documentMapper().parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("completion")
                        .field("input", "suggestion")
                        .startObject("contexts")
                            .field("location", "37.77,-122.42")
                        .endObject()
                    .endObject()
                .endObject()), XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertSuggestFields(fields, 1);
    }

    public void testParsingFull() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
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
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertThat(fields, arrayContainingInAnyOrder(
            suggestField("suggestion1"),
            suggestField("suggestion2"),
            suggestField("suggestion3")
        ));
    }

    public void testParsingMixed() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");

        ParsedDocument parsedDocument = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
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
                        .array("input", "suggestion4", "suggestion5", "suggestion6")
                        .field("weight", 5)
                        .endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(fieldMapper.name());
        assertThat(fields, arrayContainingInAnyOrder(
            suggestField("suggestion1"),
            suggestField("suggestion2"),
            suggestField("suggestion3"),
            suggestField("suggestion4"),
            suggestField("suggestion5"),
            suggestField("suggestion6")
        ));
    }

    public void testNonContextEnabledParsingWithContexts() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("field1")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        try {
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("field1")
                            .field("input", "suggestion1")
                            .startObject("contexts")
                            .field("ctx", "ctx2")
                            .endObject()
                            .field("weight", 3)
                            .endObject()
                            .endObject()),
                    XContentType.JSON));
            fail("Supplying contexts to a non context-enabled field should error");
        } catch (MapperParsingException e) {
            assertThat(e.getRootCause().getMessage(), containsString("field1"));
        }
    }

    public void testFieldValueValidation() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
        charsRefBuilder.append("sugg");
        charsRefBuilder.setCharAt(2, '\u001F');
        try {
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("completion", charsRefBuilder.get().toString())
                            .endObject()),
                    XContentType.JSON));
            fail("No error indexing value with reserved character [0x1F]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1f]"));
        }

        charsRefBuilder.setCharAt(2, '\u0000');
        try {
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("completion", charsRefBuilder.get().toString())
                            .endObject()),
                    XContentType.JSON));
            fail("No error indexing value with reserved character [0x0]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x0]"));
        }

        charsRefBuilder.setCharAt(2, '\u001E');
        try {
            defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("completion", charsRefBuilder.get().toString())
                            .endObject()),
                    XContentType.JSON));
            fail("No error indexing value with reserved character [0x1E]");
        } catch (MapperParsingException e) {
            Throwable cause = e.unwrapCause().getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1e]"));
        }

        // empty inputs are ignored
        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .array("completion", "   ", "")
                    .endObject()),
            XContentType.JSON));
        assertThat(doc.docs().size(), equalTo(1));
        assertNull(doc.docs().get(0).get("completion"));
        assertNotNull(doc.docs().get(0).getField("_ignored"));
        IndexableField ignoredFields = doc.docs().get(0).getField("_ignored");
        assertThat(ignoredFields.stringValue(), equalTo("completion"));

        // null inputs are ignored
        ParsedDocument nullDoc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .nullField("completion")
                    .endObject()),
            XContentType.JSON));
        assertThat(nullDoc.docs().size(), equalTo(1));
        assertNull(nullDoc.docs().get(0).get("completion"));
        assertNull(nullDoc.docs().get(0).getField("_ignored"));
    }

    public void testPrefixQueryType() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType().prefixQuery(new BytesRef("co"));
        assertThat(prefixQuery, instanceOf(PrefixCompletionQuery.class));
    }

    public void testFuzzyQueryType() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType().fuzzyQuery("co",
                Fuzziness.fromEdits(FuzzyCompletionQuery.DEFAULT_MAX_EDITS), FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX,
                FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH, Operations.DEFAULT_MAX_DETERMINIZED_STATES,
                FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS, FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE);
        assertThat(prefixQuery, instanceOf(FuzzyCompletionQuery.class));
    }

    public void testRegexQueryType() throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType()
                .regexpQuery(new BytesRef("co"), RegExp.ALL, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        assertThat(prefixQuery, instanceOf(RegexCompletionQuery.class));
    }

    private static void assertSuggestFields(IndexableField[] fields, int expected) {
        assertFieldsOfType(fields, SuggestField.class, expected);
    }

    private static void assertFieldsOfType(IndexableField[] fields, Class<?> clazz, int expected) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (clazz.isInstance(field)) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(expected));
    }

    public void testEmptyName() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "completion").endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testLimitOfContextMappings() throws Throwable {
        final String index = "test";
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject("properties")
            .startObject("suggest").field("type", "completion").startArray("contexts");
        for (int i = 0; i < CompletionFieldMapper.COMPLETION_CONTEXTS_LIMIT + 1; i++) {
            mappingBuilder.startObject();
            mappingBuilder.field("name", Integer.toString(i));
            mappingBuilder.field("type", "category");
            mappingBuilder.endObject();
        }

        mappingBuilder.endArray().endObject().endObject().endObject();
        String mappings = Strings.toString(mappingBuilder);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex(index).mapperService().documentMapperParser().parse("type1", new CompressedXContent(mappings));
        });
        assertTrue(e.getMessage(),
            e.getMessage().contains("Limit of completion field contexts [" +
                CompletionFieldMapper.COMPLETION_CONTEXTS_LIMIT + "] has been exceeded"));
    }

    private Matcher<IndexableField> suggestField(String value) {
        return Matchers.allOf(hasProperty(IndexableField::stringValue, equalTo(value)),
            Matchers.instanceOf(SuggestField.class));
    }

    private Matcher<IndexableField> contextSuggestField(String value) {
        return Matchers.allOf(hasProperty(IndexableField::stringValue, equalTo(value)),
            Matchers.instanceOf(ContextSuggestField.class));
    }

    private CombinableMatcher<IndexableField> sortedSetDocValuesField(String value) {
        return Matchers.both(hasProperty(IndexableField::binaryValue, equalTo(new BytesRef(value))))
            .and(Matchers.instanceOf(SortedSetDocValuesField.class));
    }

    private CombinableMatcher<IndexableField> keywordField(String value) {
        return Matchers.both(hasProperty(IndexableField::binaryValue, equalTo(new BytesRef(value))))
            .and(hasProperty(IndexableField::fieldType, Matchers.instanceOf(KeywordFieldMapper.KeywordFieldType.class)));
    }

    private <T, V> Matcher<T> hasProperty(Function<? super T, ? extends V> property, Matcher<V> valueMatcher) {
        return new FeatureMatcher<T, V>(valueMatcher, "object with", property.toString()) {
            @Override
            protected V featureValueOf(T actual) {
                return property.apply(actual);
            }
        };
    }

}
