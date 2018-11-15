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

        final Mapper fieldMapper = defaultMapper.mappers().getMapper("a_field");
        assertThat(fieldMapper, instanceOf(SearchAsYouTypeFieldMapper.class));
        final SearchAsYouTypeFieldMapper searchAsYouTypeFieldMapper = (SearchAsYouTypeFieldMapper) fieldMapper;

        final SearchAsYouTypeFieldType fieldType = searchAsYouTypeFieldMapper.fieldType();
        for (NamedAnalyzer analyzer : asList(fieldType.indexAnalyzer(), fieldType.searchAnalyzer())) {
            assertThat(analyzer.name(), equalTo("default"));
        }

        {

            final Mapper withEdgeNGramsSubfieldMapper = defaultMapper.mappers().getMapper("a_field._with_edge_ngrams");
            assertThat(withEdgeNGramsSubfieldMapper, instanceOf(SuggesterizedFieldMapper.class));
            final SuggesterizedFieldMapper withEdgeNGramsMapper = (SuggesterizedFieldMapper) withEdgeNGramsSubfieldMapper;

            final SuggesterizedFieldType withEdgeNGramsFieldType = withEdgeNGramsMapper.fieldType();
            assertThat(withEdgeNGramsFieldType.indexAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            assertThat(withEdgeNGramsFieldType.searchAnalyzer().analyzer(), not(instanceOf(SearchAsYouTypeAnalyzer.class)));
            final SearchAsYouTypeAnalyzer withEdgeNGramsIndexAnalyzer =
                (SearchAsYouTypeAnalyzer) withEdgeNGramsFieldType.indexAnalyzer().analyzer();
            final NamedAnalyzer withEdgeNGramsSearchAnalyzer = withEdgeNGramsFieldType.searchAnalyzer();

            assertFalse(withEdgeNGramsIndexAnalyzer.isWithShingles());
            assertTrue(withEdgeNGramsIndexAnalyzer.isWithEdgeNGrams());

            assertThat(withEdgeNGramsSearchAnalyzer.name(), equalTo("default"));
        }

        assertSuggesterizedFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles"), true, 2, false);

        assertSuggesterizedFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams"), true, 2, true);

        assertSuggesterizedFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles"), true, 3, false);

        assertSuggesterizedFieldMapper(
            defaultMapper.mappers().getMapper("a_field._with_3_shingles_and_edge_ngrams"), true, 3, true);
    }

    private static void assertSuggesterizedFieldMapper(Mapper mapper, boolean shingles, int numberOfShingles, boolean withEdgeNGrams) {
        assertThat(mapper, instanceOf(SuggesterizedFieldMapper.class));
        final SuggesterizedFieldMapper suggesterizedFieldMapper = (SuggesterizedFieldMapper) mapper;

        final SuggesterizedFieldType fieldType = suggesterizedFieldMapper.fieldType();
        assertThat(fieldType.indexAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        assertThat(fieldType.searchAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
        final SearchAsYouTypeAnalyzer indexAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.indexAnalyzer().analyzer();
        final SearchAsYouTypeAnalyzer searchAnalyzer = (SearchAsYouTypeAnalyzer) fieldType.searchAnalyzer().analyzer();

        for (SearchAsYouTypeAnalyzer analyzer : asList(indexAnalyzer, searchAnalyzer)) {

            assertThat(shingles, equalTo(analyzer.isWithShingles()));
            if (shingles) {
                assertThat(analyzer.getShingleSize(), equalTo(numberOfShingles));
            }
        }

        assertThat(withEdgeNGrams, equalTo(indexAnalyzer.isWithEdgeNGrams()));
    }
}
