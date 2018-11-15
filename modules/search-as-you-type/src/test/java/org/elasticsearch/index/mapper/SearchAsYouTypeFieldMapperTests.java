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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SuggesterizedFieldMapper;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SuggesterizedFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SearchAsYouTypeFieldMapperTests extends ESSingleNodeTestCase {

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

        assertTopLevelFieldMapper(defaultMapper.mappers().getMapper("a_field"), "default");

        assertEdgeNGramsFieldMapper(defaultMapper.mappers().getMapper("a_field._with_edge_ngrams"), "default");

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles"), "default", 2, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams"), "default", 2, true);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles"), "default", 3, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"), "default", 3, true);
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

        assertTopLevelFieldMapper(defaultMapper.mappers().getMapper("a_field"), "simple");

        assertEdgeNGramsFieldMapper(defaultMapper.mappers().getMapper("a_field._with_edge_ngrams"), "simple");

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles"), "simple", 2, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams"), "simple", 2, true);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles"), "simple", 3, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"), "simple", 3, true);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_4_shingles"), "simple", 4, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_4_shingles_and_edge_ngrams"), "simple", 4, true);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_5_shingles"), "simple", 5, false);

        assertShinglesAndEdgeNGramsFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_5_shingles_and_edge_ngrams"), "simple", 5, true);
    }

    private static void assertTopLevelFieldMapper(Mapper mapper, String analyzerName) {
        assertThat(mapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        final SearchAsYouTypeFieldMapper searchAsYouTypeFieldMapper = (SearchAsYouTypeFieldMapper) mapper;

        final SearchAsYouTypeFieldType fieldType = searchAsYouTypeFieldMapper.fieldType();
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }
    }

    private static void assertEdgeNGramsFieldMapper(Mapper mapper, String analyzerName) {
        assertThat(mapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper suggesterizedFieldMapper = (SuggesterizedFieldMapper) mapper;

        final SuggesterizedFieldType fieldType = suggesterizedFieldMapper.fieldType();
        final NamedAnalyzer namedIndexAnalyzer = fieldType.indexAnalyzer();
        final NamedAnalyzer namedSearchAnalyzer = fieldType.searchAnalyzer();
        assertThat(namedIndexAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        assertThat(namedSearchAnalyzer.analyzer(), not(instanceOf(SearchAsYouTypeAnalyzer.class)));
        final SearchAsYouTypeAnalyzer indexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();

        for (NamedAnalyzer analyzer : asList(namedIndexAnalyzer, namedSearchAnalyzer)) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        assertFalse(indexAnalyzer.isWithShingles());
        assertTrue(indexAnalyzer.isWithEdgeNGrams());

        assertThat(namedSearchAnalyzer.name(), equalTo(analyzerName));
    }

    private static void assertShinglesAndEdgeNGramsFieldMapper(Mapper mapper,
                                                               String analyzerName,
                                                               int numberOfShingles,
                                                               boolean withEdgeNGrams) {

        assertThat(mapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper suggesterizedFieldMapper = (SuggesterizedFieldMapper) mapper;

        final SuggesterizedFieldType fieldType = suggesterizedFieldMapper.fieldType();
        final NamedAnalyzer namedIndexAnalyzer = fieldType.indexAnalyzer();
        final NamedAnalyzer namedSearchAnalyzer = fieldType.searchAnalyzer();
        assertThat(namedIndexAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        assertThat(namedSearchAnalyzer.analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        final SearchAsYouTypeAnalyzer indexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer searchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.searchAnalyzer().analyzer();

        for (NamedAnalyzer analyzer : asList(namedIndexAnalyzer, namedSearchAnalyzer)) {
            assertThat(analyzer.name(), equalTo(analyzerName));
        }

        for (SearchAsYouTypeAnalyzer analyzer : asList(indexAnalyzer, searchAnalyzer)) {
            assertTrue(analyzer.isWithShingles());
            assertThat(analyzer.getShingleSize(), equalTo(numberOfShingles));
        }

        assertThat(indexAnalyzer.isWithEdgeNGrams(), equalTo(withEdgeNGrams));
    }
}
