/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.matchonlytext.mapper;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.matchonlytext.MatchOnlyTextMapperPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MatchOnlyTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MatchOnlyTextMapperPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    public final void testExists() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> { minimalMapping(b); }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().getSearchAnalyzer().name()));
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
            b.field("search_quote_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().getSearchQuoteAnalyzer().name()));

        checker.registerConflictCheck("analyzer", b -> b.field("analyzer", "keyword"));
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        NamedAnalyzer dflt = new NamedAnalyzer(
            "default",
            AnalyzerScope.INDEX,
            new StandardAnalyzer(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
        );
        NamedAnalyzer standard = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        NamedAnalyzer whitespace = new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer());
        NamedAnalyzer stop = new NamedAnalyzer(
            "my_stop_analyzer",
            AnalyzerScope.INDEX,
            new CustomAnalyzer(
                new StandardTokenizerFactory(indexSettings, null, "standard", indexSettings.getSettings()),
                new CharFilterFactory[0],
                new TokenFilterFactory[] { new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return "stop";
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return new StopFilter(tokenStream, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
                    }
                } }
            )
        );
        return new IndexAnalyzers(
            Map.of("default", dflt, "standard", standard, "keyword", keyword, "whitespace", whitespace, "my_stop_analyzer", stop),
            Map.of(),
            Map.of()
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "match_only_text");
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("1234", fields[0].stringValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testSearchAnalyzerSerialization() throws IOException {
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "match_only_text").field("analyzer", "standard").field("search_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default index analyzer
        mapping = fieldMapping(b -> b.field("type", "match_only_text").field("analyzer", "default").field("search_analyzer", "keyword"));
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default search analyzer
        mapping = fieldMapping(b -> b.field("type", "match_only_text").field("analyzer", "keyword").field("search_analyzer", "default"));
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        createDocumentMapper(fieldMapping(this::minimalMapping)).toXContent(
            builder,
            new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"))
        );
        builder.endObject();
        String mappingString = Strings.toString(builder);
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    public void testSearchQuoteAnalyzerSerialization() throws IOException {
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "match_only_text")
                .field("analyzer", "standard")
                .field("search_analyzer", "standard")
                .field("search_quote_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default index/search analyzer
        mapping = fieldMapping(
            b -> b.field("type", "match_only_text")
                .field("analyzer", "default")
                .field("search_analyzer", "default")
                .field("search_quote_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());
    }

    public void testNullConfigValuesFail() throws MapperParsingException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "match_only_text").field("analyzer", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[analyzer] on mapper [field] of type [match_only_text] must not have a [null] value"));
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "match_only_text").field("analyzer", "whitespace"));
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        XContentBuilder differentAnalyzer = fieldMapping(b -> b.field("type", "match_only_text").field("analyzer", "keyword"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, differentAnalyzer));
        assertThat(e.getMessage(), containsString("Cannot update parameter [analyzer]"));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field")
                .field("type", "match_only_text")
                .field("analyzer", "whitespace")
                .startObject("meta")
                .field("key", "value")
                .endObject()
                .endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    public void testDisabledSource() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("foo");
                {
                    mapping.field("type", "match_only_text");
                }
                mapping.endObject();
            }
            mapping.endObject();

            mapping.startObject("_source");
            {
                mapping.field("enabled", false);
            }
            mapping.endObject();
        }
        mapping.endObject().endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType ft = mapperService.fieldType("foo");
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.phraseQuery(ts, 0, true, context));
        assertThat(e.getMessage(), Matchers.containsString("cannot run positional queries since [_source] is disabled"));

        // Term queries are ok
        ft.termQuery("a", context); // no exception
    }
}
