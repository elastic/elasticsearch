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
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SearchAsYouTypeFieldMappersTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(SearchAsYouTypePlugin.class);
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

        assertEdgeNGramsFieldMapper(defaultMapper.mappers().getMapper("a_field._with_edge_ngrams"), topLevelFieldMapper, "default");

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "default", 2);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "default", 3);
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

        assertEdgeNGramsFieldMapper(defaultMapper.mappers().getMapper("a_field._with_edge_ngrams"), topLevelFieldMapper, "simple");

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "simple", 2);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "simple", 3);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_4_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_4_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "simple", 4);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_5_shingles"),
            defaultMapper.mappers().getMapper("a_field._with_5_shingles_and_edge_ngrams"),
            topLevelFieldMapper, "simple", 5);
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
            SourceToParse.source("test", "_doc", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("a_field", value)
                    .endObject()),
                XContentType.JSON));

        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field"), value, false, -1, false);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_edge_ngrams"), value, false, -1, true);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_2_shingles"), value, true, 2, false);
        assertIndexableFields(parsedDocument.rootDoc().getFields("a_field._with_2_shingles_and_edge_ngrams"), value, true, 2, true);
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

        assertThat(mapper.subfield(false, -1, false).fieldType(), equalTo(fieldType));
    }

    private static void assertEdgeNGramsFieldMapper(Mapper mapper, SearchAsYouTypeFieldMapper topLevelFieldMapper, String analyzerName) {
        assertThat(mapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper suggesterizedFieldMapper = (SuggesterizedFieldMapper) mapper;

        final SuggesterizedFieldType fieldType = suggesterizedFieldMapper.fieldType();
        assertThat(topLevelFieldMapper.subfield(false, -1, true).fieldType(), equalTo(fieldType));
        assertFieldType(fieldType, false, -1, true);
        assertAnalyzer(fieldType, analyzerName, false, -1, true);
    }

    private static void assertShinglesAndEdgeNGramsFieldMapper(Mapper withShinglesMapper,
                                                               Mapper withShinglesAndEdgeNGramsMapper,
                                                               SearchAsYouTypeFieldMapper topLevelFieldMapper,
                                                               String analyzerName,
                                                               int numberOfShingles) {

        assertThat(withShinglesMapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper withShinglesFieldMapper = (SuggesterizedFieldMapper) withShinglesMapper;
        final SuggesterizedFieldType withShinglesFieldType = withShinglesFieldMapper.fieldType();

        assertThat(withShinglesAndEdgeNGramsMapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper withShinglesAndEdgeNGramsFieldMapper = (SuggesterizedFieldMapper) withShinglesAndEdgeNGramsMapper;
        final SuggesterizedFieldType withShinglesAndEdgeNGramsFieldType = withShinglesAndEdgeNGramsFieldMapper.fieldType();

        assertFieldType(withShinglesFieldType, true, numberOfShingles, false);
        assertFieldType(withShinglesAndEdgeNGramsFieldType, true, numberOfShingles, true);

        assertThat(topLevelFieldMapper.subfield(true, numberOfShingles, false).fieldType(), equalTo(withShinglesFieldType));
        assertThat(topLevelFieldMapper.subfield(true, numberOfShingles, true).fieldType(), equalTo(withShinglesAndEdgeNGramsFieldType));

        assertThat(withShinglesFieldType.withEdgeNGramsField(), equalTo(withShinglesAndEdgeNGramsFieldType));
        assertThat(withShinglesAndEdgeNGramsFieldType.withEdgeNGramsField(), nullValue());

        assertThat(withShinglesFieldMapper.withEdgeNGramsField(), equalTo(withShinglesAndEdgeNGramsFieldMapper));
        assertThat(withShinglesAndEdgeNGramsFieldMapper.withEdgeNGramsField(), nullValue());

        assertAnalyzer(withShinglesFieldType, analyzerName, true, numberOfShingles, false);
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
