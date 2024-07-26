/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.suggest.document.Completion99PostingsFormat;
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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CompletionFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return Map.of("input", "value");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "completion");
    }

    @Override
    protected void metaMapping(XContentBuilder b) throws IOException {
        b.field("type", "completion");
        b.field("analyzer", "simple");
        b.field("preserve_separators", true);
        b.field("preserve_position_increments", true);
        b.field("max_input_length", 50);
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("analyzer", b -> b.field("analyzer", "standard"));
        checker.registerConflictCheck("preserve_separators", b -> b.field("preserve_separators", false));
        checker.registerConflictCheck("preserve_position_increments", b -> b.field("preserve_position_increments", false));
        checker.registerConflictCheck("contexts", b -> {
            b.startArray("contexts");
            {
                b.startObject();
                b.field("name", "place_type");
                b.field("type", "category");
                b.field("path", "cat");
                b.endObject();
            }
            b.endArray();
        });

        checker.registerUpdateCheck(
            b -> b.field("search_analyzer", "standard"),
            m -> assertEquals("standard", m.fieldType().getTextSearchInfo().searchAnalyzer().name())
        );
        checker.registerUpdateCheck(b -> b.field("max_input_length", 30), m -> {
            CompletionFieldMapper cfm = (CompletionFieldMapper) m;
            assertEquals(30, cfm.getMaxInputLength());
        });
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "standard",
                new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "simple",
                new NamedAnalyzer("simple", AnalyzerScope.INDEX, new SimpleAnalyzer())
            )
        );
    }

    public void testPostingsFormat() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG.isEnabled()) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            assertThat(((PerFieldMapperCodec) codec).getPostingsFormatForField("field"), instanceOf(Completion99PostingsFormat.class));
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            assertThat(
                ((LegacyPerFieldMapperCodec) codec).getPostingsFormatForField("field"),
                instanceOf(Completion99PostingsFormat.class)
            );
        }
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));
        MappedFieldType completionFieldType = ((CompletionFieldMapper) fieldMapper).fieldType();

        NamedAnalyzer indexAnalyzer = ((CompletionFieldMapper) fieldMapper).indexAnalyzers().values().iterator().next();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        CompletionAnalyzer analyzer = (CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));

        NamedAnalyzer searchAnalyzer = completionFieldType.getTextSearchInfo().searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("simple"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        analyzer = (CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(true));
    }

    public void testCompletionAnalyzerSettings() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.field("analyzer", "simple");
            b.field("search_analyzer", "standard");
            b.field("preserve_separators", false);
            b.field("preserve_position_increments", true);
        }));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));
        MappedFieldType completionFieldType = ((CompletionFieldMapper) fieldMapper).fieldType();

        NamedAnalyzer indexAnalyzer = ((CompletionFieldMapper) fieldMapper).indexAnalyzers().values().iterator().next();
        assertThat(indexAnalyzer.name(), equalTo("simple"));
        assertThat(indexAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        CompletionAnalyzer analyzer = (CompletionAnalyzer) indexAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

        NamedAnalyzer searchAnalyzer = completionFieldType.getTextSearchInfo().searchAnalyzer();
        assertThat(searchAnalyzer.name(), equalTo("standard"));
        assertThat(searchAnalyzer.analyzer(), instanceOf(CompletionAnalyzer.class));
        analyzer = (CompletionAnalyzer) searchAnalyzer.analyzer();
        assertThat(analyzer.preservePositionIncrements(), equalTo(true));
        assertThat(analyzer.preserveSep(), equalTo(false));

        assertEquals("""
            {"field":{"type":"completion","analyzer":"simple","search_analyzer":"standard",\
            "preserve_separators":false,"preserve_position_increments":true,"max_input_length":50}}""", Strings.toString(fieldMapper));
    }

    @SuppressWarnings("unchecked")
    public void testTypeParsing() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.field("analyzer", "simple");
            b.field("search_analyzer", "standard");
            b.field("preserve_separators", false);
            b.field("preserve_position_increments", true);
            b.field("max_input_length", 14);
        }));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper.class));

        XContentBuilder builder = jsonBuilder().startObject();
        fieldMapper.toXContent(builder, new ToXContent.MapParams(Map.of("include_defaults", "true"))).endObject();
        builder.close();
        Map<String, Object> serializedMap;
        try (var parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            serializedMap = parser.map();
        }
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("field");
        assertThat(configMap.get("analyzer").toString(), is("simple"));
        assertThat(configMap.get("search_analyzer").toString(), is("standard"));
        assertThat(Boolean.valueOf(configMap.get("preserve_separators").toString()), is(false));
        assertThat(Boolean.valueOf(configMap.get("preserve_position_increments").toString()), is(true));
        assertThat(Integer.valueOf(configMap.get("max_input_length").toString()), is(14));
    }

    public void testParsingMinimal() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.field("field", "suggestion")));
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertFieldsOfType(fields);
    }

    public void testParsingFailure() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(source(b -> b.field("field", 1.0)))
        );
        assertEquals("failed to parse [field]: expected text or object, but got VALUE_NUMBER", e.getCause().getMessage());
    }

    public void testKeywordWithSubCompletionAndContext() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            {
                b.startObject("subsuggest");
                {
                    b.field("type", "completion");
                    b.startArray("contexts");
                    {
                        b.startObject();
                        b.field("name", "place_type");
                        b.field("type", "category");
                        b.field("path", "cat");
                        b.endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.array("field", "key1", "key2", "key3")));

        LuceneDocument indexableFields = parsedDocument.rootDoc();

        assertThat(
            indexableFields.getFields("field"),
            containsInAnyOrder(keywordField("key1"), keywordField("key2"), keywordField("key3"))
        );
        assertThat(
            indexableFields.getFields("field.subsuggest"),
            containsInAnyOrder(contextSuggestField("key1"), contextSuggestField("key2"), contextSuggestField("key3"))
        );
    }

    public void testCompletionWithContextAndSubCompletion() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.startArray("contexts");
            {
                b.startObject();
                b.field("name", "place_type");
                b.field("type", "category");
                b.field("path", "cat");
                b.endObject();
            }
            b.endArray();
            b.startObject("fields");
            {
                b.startObject("subsuggest");
                {
                    b.field("type", "completion");
                    b.startArray("contexts");
                    {
                        b.startObject();
                        b.field("name", "place_type");
                        b.field("type", "category");
                        b.field("path", "cat");
                        b.endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endObject();
        }));

        {
            ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
                b.startObject("field");
                {
                    b.array("input", "timmy", "starbucks");
                    b.startObject("contexts").array("place_type", "cafe", "food").endObject();
                    b.field("weight", 3);
                }
                b.endObject();
            }));

            LuceneDocument indexableFields = parsedDocument.rootDoc();
            assertThat(
                indexableFields.getFields("field"),
                containsInAnyOrder(contextSuggestField("timmy"), contextSuggestField("starbucks"))
            );
            assertThat(
                indexableFields.getFields("field.subsuggest"),
                containsInAnyOrder(contextSuggestField("timmy"), contextSuggestField("starbucks"))
            );
            // check that the indexable fields produce tokenstreams without throwing an exception
            // if this breaks it is likely a problem with setting contexts
            try (TokenStream ts = indexableFields.getFields("field.subsuggest").get(0).tokenStream(Lucene.WHITESPACE_ANALYZER, null)) {
                ts.reset();
                while (ts.incrementToken()) {
                }
                ts.end();
            }
            // unable to assert about context, covered in a REST test
        }

        {
            ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
                b.array("field", "timmy", "starbucks");
                b.array("cat", "cafe", "food");
            }));

            LuceneDocument indexableFields = parsedDocument.rootDoc();
            assertThat(
                indexableFields.getFields("field"),
                containsInAnyOrder(contextSuggestField("timmy"), contextSuggestField("starbucks"))
            );
            assertThat(
                indexableFields.getFields("field.subsuggest"),
                containsInAnyOrder(contextSuggestField("timmy"), contextSuggestField("starbucks"))
            );
            // unable to assert about context, covered in a REST test
        }
    }

    public void testGeoHashWithSubCompletionAndStringInsert() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point");
            b.startObject("fields");
            {
                b.startObject("analyzed").field("type", "completion").endObject();
            }
            b.endObject();
        }));

        // "41.12,-71.34"
        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.field("field", "drm3btev3e86")));

        LuceneDocument indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("field"), hasSize(2));
        assertThat(indexableFields.getFields("field.analyzed"), containsInAnyOrder(suggestField("drm3btev3e86")));
        // unable to assert about geofield content, covered in a REST test
    }

    public void testCompletionTypeWithSubfieldsAndStringInsert() throws Exception {
        List<CheckedConsumer<XContentBuilder, IOException>> builders = new ArrayList<>();
        builders.add(b -> b.startObject("analyzed1").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("analyzed2").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("subsuggest1").field("type", "completion").endObject());
        builders.add(b -> b.startObject("subsuggest2").field("type", "completion").endObject());
        Collections.shuffle(builders, random());

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.startObject("fields");
            for (CheckedConsumer<XContentBuilder, IOException> builder : builders) {
                builder.accept(b);
            }
            b.endObject();
        }));

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.field("field", "suggestion")));

        LuceneDocument indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("field"), containsInAnyOrder(suggestField("suggestion")));
        assertThat(indexableFields.getFields("field.subsuggest1"), containsInAnyOrder(suggestField("suggestion")));
        assertThat(indexableFields.getFields("field.subsuggest2"), containsInAnyOrder(suggestField("suggestion")));
        assertThat(indexableFields.getFields("field.analyzed1"), containsInAnyOrder(keywordField("suggestion")));
        assertThat(indexableFields.getFields("field.analyzed2"), containsInAnyOrder(keywordField("suggestion")));
    }

    public void testCompletionTypeWithSubfieldsAndArrayInsert() throws Exception {
        List<CheckedConsumer<XContentBuilder, IOException>> builders = new ArrayList<>();
        builders.add(b -> b.startObject("analyzed1").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("analyzed2").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("subcompletion1").field("type", "completion").endObject());
        builders.add(b -> b.startObject("subcompletion2").field("type", "completion").endObject());
        Collections.shuffle(builders, random());

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.startObject("fields");
            for (CheckedConsumer<XContentBuilder, IOException> builder : builders) {
                builder.accept(b);
            }
            b.endObject();
        }));

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.array("field", "New York", "NY")));

        LuceneDocument indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("field"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.subcompletion1"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.subcompletion2"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.analyzed1"), containsInAnyOrder(keywordField("New York"), keywordField("NY")));
        assertThat(indexableFields.getFields("field.analyzed2"), containsInAnyOrder(keywordField("New York"), keywordField("NY")));
    }

    public void testCompletionTypeWithSubfieldsAndObjectInsert() throws Exception {
        List<CheckedConsumer<XContentBuilder, IOException>> builders = new ArrayList<>();
        builders.add(b -> b.startObject("analyzed1").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("analyzed2").field("type", "keyword").endObject());
        builders.add(b -> b.startObject("subcompletion1").field("type", "completion").endObject());
        builders.add(b -> b.startObject("subcompletion2").field("type", "completion").endObject());
        Collections.shuffle(builders, random());

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.startObject("fields");
            for (CheckedConsumer<XContentBuilder, IOException> builder : builders) {
                builder.accept(b);
            }
            b.endObject();
        }));

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.array("input", "New York", "NY");
                b.field("weight", 34);
            }
            b.endObject();
        }));

        LuceneDocument indexableFields = parsedDocument.rootDoc();
        assertThat(indexableFields.getFields("field"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.subcompletion1"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.subcompletion2"), containsInAnyOrder(suggestField("New York"), suggestField("NY")));
        assertThat(indexableFields.getFields("field.analyzed1"), containsInAnyOrder(keywordField("New York"), keywordField("NY")));
        assertThat(indexableFields.getFields("field.analyzed2"), containsInAnyOrder(keywordField("New York"), keywordField("NY")));
        // unable to assert about weight, covered in a REST test
    }

    public void testParsingMultiValued() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> b.array("field", "suggestion1", "suggestion2")));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertThat(fields, containsInAnyOrder(suggestField("suggestion1"), suggestField("suggestion2")));
    }

    public void testParsingWithWeight() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.field("input", "suggestion");
                b.field("weight", 2);
            }
            b.endObject();
        }));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertThat(fields, containsInAnyOrder(suggestField("suggestion")));
    }

    public void testParsingMultiValueWithWeight() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.array("input", "suggestion1", "suggestion2", "suggestion3");
                b.field("weight", 2);
            }
            b.endObject();
        }));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertThat(fields, containsInAnyOrder(suggestField("suggestion1"), suggestField("suggestion2"), suggestField("suggestion3")));
    }

    public void testParsingWithGeoFieldAlias() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("completion");
            {
                b.field("type", "completion");
                b.startObject("contexts");
                {
                    b.field("name", "location");
                    b.field("type", "geo");
                    b.field("path", "alias");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("birth-place").field("type", "geo_point").endObject();
            b.startObject("alias");
            {
                b.field("type", "alias");
                b.field("path", "birth-place");
            }
            b.endObject();
        }));
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper("completion");

        ParsedDocument parsedDocument = mapperService.documentMapper().parse(source(b -> {
            b.startObject("completion");
            {
                b.field("input", "suggestion");
                b.startObject("contexts").field("location", "37.77,-122.42").endObject();
            }
            b.endObject();
        }));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertFieldsOfType(fields);
    }

    public void testParsingFull() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("input", "suggestion1").field("weight", 3).endObject();
                b.startObject().field("input", "suggestion2").field("weight", 4).endObject();
                b.startObject().field("input", "suggestion3").field("weight", 5).endObject();
            }
            b.endArray();
        }));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertThat(fields, containsInAnyOrder(suggestField("suggestion1"), suggestField("suggestion2"), suggestField("suggestion3")));
    }

    public void testParsingMixed() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");

        ParsedDocument parsedDocument = defaultMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject();
                {
                    b.array("input", "suggestion1", "suggestion2");
                    b.field("weight", 3);
                }
                b.endObject();
                b.startObject();
                {
                    b.array("input", "suggestion3");
                    b.field("weight", 4);
                }
                b.endObject();
                b.startObject();
                {
                    b.array("input", "suggestion4", "suggestion5", "suggestion6");
                    b.field("weight", 5);
                }
                b.endObject();
            }
            b.endArray();
        }));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields(fieldMapper.fullPath());
        assertThat(
            fields,
            containsInAnyOrder(
                suggestField("suggestion1"),
                suggestField("suggestion2"),
                suggestField("suggestion3"),
                suggestField("suggestion4"),
                suggestField("suggestion5"),
                suggestField("suggestion6")
            )
        );
    }

    public void testNonContextEnabledParsingWithContexts() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.field("input", "suggestion1");
                b.startObject("contexts").field("ctx", "ctx2").endObject();
                b.field("weight", 3);
            }
            b.endObject();
        })));

        assertThat(e.getMessage(), containsString("field"));
    }

    public void testFieldValueValidation() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
        charsRefBuilder.append("sugg");
        charsRefBuilder.setCharAt(2, '\u001F');

        {
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", charsRefBuilder.get().toString())))
            );

            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1f]"));
        }

        charsRefBuilder.setCharAt(2, '\u0000');
        {
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", charsRefBuilder.get().toString())))
            );

            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x0]"));
        }

        charsRefBuilder.setCharAt(2, '\u001E');
        {
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", charsRefBuilder.get().toString())))
            );

            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), containsString("[0x1e]"));
        }

        // empty inputs are ignored
        ParsedDocument doc = defaultMapper.parse(source(b -> b.array("field", "    ", "")));
        assertThat(doc.docs().size(), equalTo(1));
        assertNull(doc.docs().get(0).get("field"));
        assertNotNull(doc.docs().get(0).getField("_ignored"));
        List<IndexableField> ignoredFields = doc.docs().get(0).getFields("_ignored");
        assertTrue(ignoredFields.stream().anyMatch(field -> "field".equals(field.stringValue())));

        // null inputs are ignored
        ParsedDocument nullDoc = defaultMapper.parse(source(b -> b.nullField("field")));
        assertThat(nullDoc.docs().size(), equalTo(1));
        assertNull(nullDoc.docs().get(0).get("field"));
        assertNull(nullDoc.docs().get(0).getField("_ignored"));
    }

    public void testPrefixQueryType() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType().prefixQuery(new BytesRef("co"));
        assertThat(prefixQuery, instanceOf(PrefixCompletionQuery.class));
    }

    public void testFuzzyQueryType() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType()
            .fuzzyQuery(
                "co",
                Fuzziness.fromEdits(FuzzyCompletionQuery.DEFAULT_MAX_EDITS),
                FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX,
                FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH,
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS,
                FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE
            );
        assertThat(prefixQuery, instanceOf(FuzzyCompletionQuery.class));
    }

    public void testRegexQueryType() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("field");
        CompletionFieldMapper completionFieldMapper = (CompletionFieldMapper) fieldMapper;
        Query prefixQuery = completionFieldMapper.fieldType()
            .regexpQuery(new BytesRef("co"), RegExp.ALL, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertThat(prefixQuery, instanceOf(RegexCompletionQuery.class));
    }

    private static void assertFieldsOfType(List<IndexableField> fields) {
        int actualFieldCount = 0;
        for (IndexableField field : fields) {
            if (field instanceof SuggestField) {
                actualFieldCount++;
            }
        }
        assertThat(actualFieldCount, equalTo(1));
    }

    public void testLimitOfContextMappings() throws Throwable {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("suggest")
            .field("type", "completion")
            .startArray("contexts");
        for (int i = 0; i < CompletionFieldMapper.COMPLETION_CONTEXTS_LIMIT + 1; i++) {
            mappingBuilder.startObject();
            mappingBuilder.field("name", Integer.toString(i));
            mappingBuilder.field("type", "category");
            mappingBuilder.endObject();
        }

        mappingBuilder.endArray().endObject().endObject().endObject();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "completion");
            b.startArray("contexts");
            for (int i = 0; i < CompletionFieldMapper.COMPLETION_CONTEXTS_LIMIT + 1; i++) {
                b.startObject();
                b.field("name", Integer.toString(i));
                b.field("type", "category");
                b.endObject();
            }
            b.endArray();
        })));
        assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains("Limit of completion field contexts [" + CompletionFieldMapper.COMPLETION_CONTEXTS_LIMIT + "] has been exceeded")
        );
    }

    private static CompletionFieldMapper.CompletionInputMetadata randomCompletionMetadata() {
        Map<String, Set<String>> contexts = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap("filter", Collections.singleton("value"));
        return new CompletionFieldMapper.CompletionInputMetadata("text", contexts, 10);
    }

    private static XContentParser documentParser(CompletionFieldMapper.CompletionInputMetadata metadata) throws IOException {
        XContentBuilder docBuilder = JsonXContent.contentBuilder();
        if (randomBoolean()) {
            docBuilder.prettyPrint();
        }
        docBuilder.startObject();
        docBuilder.field("field");
        docBuilder.map(metadata.toMap());
        docBuilder.endObject();
        String document = Strings.toString(docBuilder);
        XContentParser docParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, document);
        docParser.nextToken();
        docParser.nextToken();
        assertEquals(XContentParser.Token.START_OBJECT, docParser.nextToken());
        return docParser;
    }

    public void testMultiFieldParserSimpleValue() throws IOException {
        CompletionFieldMapper.CompletionInputMetadata metadata = randomCompletionMetadata();
        XContentParser documentParser = documentParser(metadata);
        XContentParser multiFieldParser = new CompletionFieldMapper.MultiFieldParser(
            metadata,
            documentParser.currentName(),
            documentParser.getTokenLocation()
        );
        // we don't check currentToken here because it returns START_OBJECT that is inconsistent with returning a value
        assertEquals("text", multiFieldParser.textOrNull());
        assertEquals(documentParser.getTokenLocation(), multiFieldParser.getTokenLocation());
        assertEquals(documentParser.currentName(), multiFieldParser.currentName());
    }

    public void testMultiFieldParserCompletionSubfield() throws IOException {
        CompletionFieldMapper.CompletionInputMetadata metadata = randomCompletionMetadata();
        XContentParser documentParser = documentParser(metadata);
        // compare the object structure with the original metadata, this implicitly verifies that the xcontent read is valid
        XContentBuilder multiFieldBuilder = JsonXContent.contentBuilder()
            .copyCurrentStructure(
                new CompletionFieldMapper.MultiFieldParser(metadata, documentParser.currentName(), documentParser.getTokenLocation())
            );
        XContentBuilder metadataBuilder = JsonXContent.contentBuilder().map(metadata.toMap());
        String jsonMetadata = Strings.toString(metadataBuilder);
        assertEquals(jsonMetadata, Strings.toString(multiFieldBuilder));
        // advance token by token and verify currentName as well as getTokenLocation
        XContentParser multiFieldParser = new CompletionFieldMapper.MultiFieldParser(
            metadata,
            documentParser.currentName(),
            documentParser.getTokenLocation()
        );
        XContentParser expectedParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonMetadata);
        assertEquals(expectedParser.nextToken(), multiFieldParser.currentToken());
        XContentLocation expectedTokenLocation = documentParser.getTokenLocation();
        while (expectedParser.nextToken() != null) {
            XContentParser.Token token = multiFieldParser.nextToken();
            assertEquals(expectedParser.currentToken(), token);
            assertEquals(expectedParser.currentToken(), multiFieldParser.currentToken());
            assertEquals(expectedTokenLocation, multiFieldParser.getTokenLocation());
            assertEquals(documentParser.nextToken(), multiFieldParser.currentToken());
            assertEquals(documentParser.currentName(), multiFieldParser.currentName());
        }
        assertNull(multiFieldParser.nextToken());
    }

    public void testMultiFieldParserMixedSubfields() throws IOException {
        CompletionFieldMapper.CompletionInputMetadata metadata = randomCompletionMetadata();
        XContentParser documentParser = documentParser(metadata);
        // simulate 10 sub-fields which may either read simple values or the full object structure
        for (int i = 0; i < 10; i++) {
            XContentParser multiFieldParser = new CompletionFieldMapper.MultiFieldParser(
                metadata,
                documentParser.currentName(),
                documentParser.getTokenLocation()
            );
            if (randomBoolean()) {
                assertEquals("text", multiFieldParser.textOrNull());
            } else {
                XContentBuilder multiFieldBuilder = JsonXContent.contentBuilder().copyCurrentStructure(multiFieldParser);
                XContentBuilder metadataBuilder = JsonXContent.contentBuilder().map(metadata.toMap());
                String jsonMetadata = Strings.toString(metadataBuilder);
                assertEquals(jsonMetadata, Strings.toString(multiFieldBuilder));
            }
        }
    }

    private Matcher<IndexableField> suggestField(String value) {
        return Matchers.allOf(hasProperty(IndexableField::stringValue, equalTo(value)), Matchers.instanceOf(SuggestField.class));
    }

    private Matcher<IndexableField> contextSuggestField(String value) {
        return Matchers.allOf(hasProperty(IndexableField::stringValue, equalTo(value)), Matchers.instanceOf(ContextSuggestField.class));
    }

    private Matcher<IndexableField> keywordField(String value) {
        return Matchers.allOf(
            hasProperty(IndexableField::binaryValue, equalTo(new BytesRef(value))),
            hasProperty(ft -> ft.fieldType().indexOptions(), equalTo(IndexOptions.DOCS)),
            hasProperty(ft -> ft.fieldType().docValuesType(), equalTo(DocValuesType.SORTED_SET))
        );
    }

    private <T, V> Matcher<T> hasProperty(Function<? super T, ? extends V> property, Matcher<V> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "object with", property.toString()) {
            @Override
            protected V featureValueOf(T actual) {
                return property.apply(actual);
            }
        };
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have doc values or fielddata", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
