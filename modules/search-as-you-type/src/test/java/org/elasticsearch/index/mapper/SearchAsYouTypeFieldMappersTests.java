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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMappers.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMappers.SearchAsYouTypeFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMappers.SuggesterizedFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMappers.SuggesterizedFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchAsYouTypeFieldMappersTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(SearchAsYouTypePlugin.class);
    }

    // todo remove this is temporary
    public void testAnalysisV1() throws IOException {
        final SimpleAnalyzer simple = new SimpleAnalyzer();
        final SearchAsYouTypeAnalyzer withEdgeNGrams = SearchAsYouTypeAnalyzer.withEdgeNGrams(simple);
        final SearchAsYouTypeAnalyzer with2Shingles = SearchAsYouTypeAnalyzer.withShingles(simple, 2);
        final SearchAsYouTypeAnalyzer with2ShinglesAndEdgeNGrams = SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(simple, 2);
        final SearchAsYouTypeAnalyzer with3Shingles = SearchAsYouTypeAnalyzer.withShingles(simple, 3);
        final SearchAsYouTypeAnalyzer with3ShinglesAndEdgeNGrams = SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(simple, 3);

        final String text = "aa bb cc dd ee";

        final Map<String, List<String>> analyzerToTerm = new HashMap<>();
        final Map<String, Set<String>> termToAnalyzers = new HashMap<>();

        analyze("root", simple, text, analyzerToTerm, termToAnalyzers);
        analyze("with_edge_ngrams", withEdgeNGrams, text, analyzerToTerm, termToAnalyzers);
        analyze("with_2_shingles", with2Shingles, text, analyzerToTerm, termToAnalyzers);
        analyze("with_2_shingles_and_edge_ngrams", with2ShinglesAndEdgeNGrams, text, analyzerToTerm, termToAnalyzers);
        analyze("with_3_shingles", with3Shingles, text, analyzerToTerm, termToAnalyzers);
        analyze("with_3_shingles_and_edge_ngrams", with3ShinglesAndEdgeNGrams, text, analyzerToTerm, termToAnalyzers);

        logger.error("#############\nTokens by analyzer\n######################\n");
        for (Map.Entry<String, List<String>> entry : analyzerToTerm.entrySet()) {
            logger.error("Tokens for analyzer [" + entry.getKey() + "] are [" + entry.getValue().toString() + "]");
        }

        logger.error("#################\nAnalyzers by token\n####################\n");
        for (Map.Entry<String, Set<String>> entry : termToAnalyzers.entrySet()) {
            logger.error("Analyzers with token [" + entry.getKey() + "] are [" + entry.getValue().toString() + "]");
        }
    }

    // todo remove this is temporary
    public void testAnalysisV2() throws IOException {
        final SimpleAnalyzer simple = new SimpleAnalyzer();
        final SearchAsYouTypeAnalyzer with2Shingles = SearchAsYouTypeAnalyzer.withShingles(simple, 2);
        final SearchAsYouTypeAnalyzer with3Shingles = SearchAsYouTypeAnalyzer.withShingles(simple, 3);
        final SearchAsYouTypeAnalyzer with3ShinglesAndEdgeNGrams = SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(simple, 3);

        final String text = "aa bb cc dd ee";

        final Map<String, List<String>> analyzerToTerm = new HashMap<>();
        final Map<String, Set<String>> termToAnalyzers = new HashMap<>();

        analyze("root", simple, text, analyzerToTerm, termToAnalyzers);
        analyze("with_2_shingles", with2Shingles, text, analyzerToTerm, termToAnalyzers);
        analyze("with_3_shingles", with3Shingles, text, analyzerToTerm, termToAnalyzers);
        analyze("with_3_shingles_and_edge_ngrams", with3ShinglesAndEdgeNGrams, text, analyzerToTerm, termToAnalyzers);

        logger.error("#############\nTokens by analyzer\n######################\n");
        for (Map.Entry<String, List<String>> entry : analyzerToTerm.entrySet()) {
            logger.error("Tokens for analyzer [" + entry.getKey() + "] are [" + entry.getValue().toString() + "]");
        }

        logger.error("#################\nAnalyzers by token\n####################\n");
        for (Map.Entry<String, Set<String>> entry : termToAnalyzers.entrySet()) {
            logger.error("Analyzers with token [" + entry.getKey() + "] are [" + entry.getValue().toString() + "]");
        }
    }

    // todo remove this is temporary
    private static void analyze(String name,
                                Analyzer analyzer,
                                String text,
                                Map<String, List<String>> analyzerToTerm,
                                Map<String, Set<String>> termToAnalyzers) throws IOException {
        final List<String> tokens = analyzeTerms(text, analyzer);
        assertFalse(analyzerToTerm.containsKey(name));
        analyzerToTerm.put(name, tokens);
        for (String token : tokens) {
            if (termToAnalyzers.containsKey(token) == false) {
                termToAnalyzers.put(token, new HashSet<>());
            }
            termToAnalyzers.get(token).add(name);
        }
    }

    // todo remove this is temporary
    private static List<String> analyzeTerms(String text, Analyzer analyzer) throws IOException {
        final List<String> terms = new ArrayList<>();
        try (TokenStream tokenStream = analyzer.tokenStream("foo", text)) {
            final CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
            tokenStream.end();
        }
        return terms;
    }

    public void testDefaultConfiguration() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final Mapper mapper = defaultMapper.mappers().getMapper("a_field");
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        final SearchAsYouTypeFieldMapper topLevelFieldMapper = (SearchAsYouTypeFieldMapper) mapper;

        assertTopLevelFieldMapper(topLevelFieldMapper, "default");

        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_2_shingles"), topLevelFieldMapper, "default", 2, false);
        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_3_shingles"), topLevelFieldMapper, "default", 3, true);
        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"), topLevelFieldMapper, "default", 3);
    }

    public void testConfiguration() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .field("analyzer", "simple")
            .field("max_shingle_size", 5)
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final Mapper mapper = defaultMapper.mappers().getMapper("a_field");
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        final SearchAsYouTypeFieldMapper topLevelFieldMapper = (SearchAsYouTypeFieldMapper) mapper;

        assertTopLevelFieldMapper(topLevelFieldMapper, "simple");

        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_2_shingles"), topLevelFieldMapper, "simple", 2, false);
        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_3_shingles"), topLevelFieldMapper, "simple", 3, false);
        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_4_shingles"), topLevelFieldMapper, "simple", 4, false);
        assertShinglesFieldMapper(defaultMapper.mappers().getMapper("a_field._with_5_shingles"), topLevelFieldMapper, "simple", 5, true);
        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_5_shingles_and_edge_ngrams"), topLevelFieldMapper, "simple", 5);
    }

    public void testDocumentParsing() throws IOException {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a_field")
            .field("type", "search_as_you_type")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        final DocumentMapper defaultMapper = createIndex("test")
            .mapperService()
            .documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        final String value = randomAlphaOfLengthBetween(5, 20);
        final ParsedDocument parsedDocument = defaultMapper.parse(
            new SourceToParse("test", "_doc", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("a_field", value)
                    .endObject()),
                XContentType.JSON));

        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field"), value, false, -1, false);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_2_shingles"), value, true, 2, false);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_3_shingles"), value, true, 3, false);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_3_shingles_and_edge_ngrams"), value, true, 3, true);
    }

    private static void assertTopLevelFieldMapper(SearchAsYouTypeFieldMapper mapper, String analyzerName) {
        final SuggesterizedFieldType fieldType = mapper.fieldType();
        assertFalse(fieldType.hasShingles());
        assertFalse(fieldType.hasEdgeNGrams());
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
    }

    private static void assertShinglesFieldMapper(Mapper fieldMapper,
                                                  SearchAsYouTypeFieldMapper rootFieldMapper,
                                                  String analyzerName,
                                                  int numberOfShingles,
                                                  boolean hasEdgeNGramsSubfield) {

        assertThat(fieldMapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper withShinglesFieldMapper = (SuggesterizedFieldMapper) fieldMapper;
        final SuggesterizedFieldType withShinglesFieldType = withShinglesFieldMapper.fieldType();

        assertFieldType(withShinglesFieldType, true, numberOfShingles, false);

        assertThat(rootFieldMapper.subfield(numberOfShingles, false).fieldType(), equalTo(withShinglesFieldType));

        if (hasEdgeNGramsSubfield) {
            assertThat(withShinglesFieldType.withEdgeNGramsField(), notNullValue());
            assertFieldType(withShinglesFieldType.withEdgeNGramsField(), true, numberOfShingles, true);
            assertThat(withShinglesFieldMapper.withEdgeNGramsField(), notNullValue());
        } else {
            assertThat(withShinglesFieldType.withEdgeNGramsField(), nullValue());
            assertThat(withShinglesFieldMapper.withEdgeNGramsField(), nullValue());
        }

        assertAnalyzer(withShinglesFieldType, analyzerName, true, numberOfShingles, false);
    }

    private static void assertShinglesAndEdgeNGramsFieldMapper(Mapper fieldMapper,
                                                               SearchAsYouTypeFieldMapper rootFieldmapper,
                                                               String analyzerName,
                                                               int numberOfShingles) {

        assertThat(fieldMapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper withShinglesAndEdgeNGramsFieldMapper = (SuggesterizedFieldMapper) fieldMapper;
        final SuggesterizedFieldType withShinglesAndEdgeNGramsFieldType = withShinglesAndEdgeNGramsFieldMapper.fieldType();

        assertFieldType(withShinglesAndEdgeNGramsFieldType, true, numberOfShingles, true);

        assertThat(rootFieldmapper.subfield(numberOfShingles, true).fieldType(), equalTo(withShinglesAndEdgeNGramsFieldType));

        assertThat(withShinglesAndEdgeNGramsFieldType.withEdgeNGramsField(), nullValue());
        assertThat(withShinglesAndEdgeNGramsFieldMapper.withEdgeNGramsField(), nullValue());

        assertAnalyzer(withShinglesAndEdgeNGramsFieldType, analyzerName, true, numberOfShingles, true);
    }

    private static void assertIndexableFields(IndexableField[] indexableFields,
                                              String value,
                                              boolean hasShingles,
                                              int shingleSize,
                                              boolean hasEdgeNGrams) {

        assertThat(indexableFields.length, equalTo(1));
        final IndexableField field = indexableFields[0];
        assertThat(field.stringValue(), equalTo(value));
        assertThat(field.fieldType(), instanceOf(SuggesterizedFieldType.class));
        final SuggesterizedFieldType fieldType = (SuggesterizedFieldType) field.fieldType();
        assertFieldType(fieldType, hasShingles, shingleSize, hasEdgeNGrams);
    }

    private static void assertFieldType(SuggesterizedFieldType fieldType,
                                        boolean hasShingles,
                                        int shingleSize,
                                        boolean hasEdgeNGrams) {

        assertThat(fieldType.hasShingles(), equalTo(hasShingles));
        if (hasShingles) {
            assertThat(fieldType.shingleSize(), equalTo(shingleSize));
        }
        assertThat(fieldType.hasEdgeNGrams(), equalTo(hasEdgeNGrams));
    }

    private static void assertAnalyzer(SuggesterizedFieldType fieldType,
                                       String analyzerName,
                                       boolean hasShingles,
                                       int shingleSize,
                                       boolean hasEdgeNGrams) {

        final NamedAnalyzer namedIndexAnalyzer = fieldType.indexAnalyzer();
        final NamedAnalyzer namedSearchAnalyzer = fieldType.searchAnalyzer();
        assertThat(namedIndexAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        assertThat(namedSearchAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        final SearchAsYouTypeAnalyzer indexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer searchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.searchAnalyzer().analyzer();

        for (NamedAnalyzer namedAnalyzer : asList(namedIndexAnalyzer, namedSearchAnalyzer)) {
            assertThat(namedAnalyzer.name(), equalTo(analyzerName));
        }

        for (SearchAsYouTypeAnalyzer analyzer : asList(indexAnalyzer, searchAnalyzer)) {
            assertThat(analyzer.hasShingles(), equalTo(hasShingles));
            if (hasShingles) {
                assertThat(analyzer.shingleSize(), equalTo(shingleSize));
            }
        }

        assertThat(indexAnalyzer.hasEdgeNGrams(), equalTo(hasEdgeNGrams));
        assertFalse(searchAnalyzer.hasEdgeNGrams());
    }
}
