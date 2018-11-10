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
            assertThat(withEdgeNGramsIndexAnalyzer.getMinGram(), equalTo(1));
            assertThat(withEdgeNGramsIndexAnalyzer.getMaxGram(), equalTo(20));

            assertThat(withEdgeNGramsSearchAnalyzer.name(), equalTo("default"));
        }

        {

            final Mapper with2ShinglesSubfieldMapper = defaultMapper.mappers().getMapper("a_field._with_2_shingles");
            assertThat(with2ShinglesSubfieldMapper, instanceOf(SuggesterizedFieldMapper.class));
            final SuggesterizedFieldMapper with2ShinglesMapper = (SuggesterizedFieldMapper) with2ShinglesSubfieldMapper;

            final SuggesterizedFieldType with2ShinglesFieldType = with2ShinglesMapper.fieldType();
            assertThat(with2ShinglesFieldType.indexAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            assertThat(with2ShinglesFieldType.searchAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            final SearchAsYouTypeAnalyzer with2ShinglesIndexAnalyzer =
                (SearchAsYouTypeAnalyzer) with2ShinglesFieldType.indexAnalyzer().analyzer();
            final SearchAsYouTypeAnalyzer with2ShinglesSearchAnalyzer =
                (SearchAsYouTypeAnalyzer) with2ShinglesFieldType.searchAnalyzer().analyzer();

            for (SearchAsYouTypeAnalyzer analyzer : asList(with2ShinglesIndexAnalyzer, with2ShinglesSearchAnalyzer)) {
                assertTrue(analyzer.isWithShingles());
                assertThat(analyzer.getShingleSize(), equalTo(2));
                assertFalse(analyzer.isWithEdgeNGrams());
            }

        }

        {

            final Mapper with2ShinglesEdgeNGramsSubfieldMapper = defaultMapper.mappers().getMapper("a_field._with_2_shingles_and_edge_ngrams");
            assertThat(with2ShinglesEdgeNGramsSubfieldMapper, instanceOf(SuggesterizedFieldMapper.class));
            final SuggesterizedFieldMapper with2ShinglesEdgeNGramsMapper = (SuggesterizedFieldMapper) with2ShinglesEdgeNGramsSubfieldMapper;

            final SuggesterizedFieldType with2ShinglesEdgeNGramsFieldType = with2ShinglesEdgeNGramsMapper.fieldType();
            assertThat(with2ShinglesEdgeNGramsFieldType.indexAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            assertThat(with2ShinglesEdgeNGramsFieldType.searchAnalyzer().analyzer(), instanceOf(SearchAsYouTypeAnalyzer.class));
            final SearchAsYouTypeAnalyzer with2ShinglesEdgeNGramsIndexAnalyzer =
                (SearchAsYouTypeAnalyzer) with2ShinglesEdgeNGramsFieldType.indexAnalyzer().analyzer();
            final SearchAsYouTypeAnalyzer with2ShinglesEdgeNGramsSearchAnalyzer =
                (SearchAsYouTypeAnalyzer) with2ShinglesEdgeNGramsFieldType.searchAnalyzer().analyzer();

            for (SearchAsYouTypeAnalyzer analyzer : asList(with2ShinglesEdgeNGramsIndexAnalyzer, with2ShinglesEdgeNGramsSearchAnalyzer)) {
                assertTrue(analyzer.isWithShingles());
                assertThat(analyzer.getShingleSize(), equalTo(2));
            }

            assertTrue(with2ShinglesEdgeNGramsIndexAnalyzer.isWithEdgeNGrams());
            assertThat(with2ShinglesEdgeNGramsIndexAnalyzer.getMinGram(), equalTo(1));
            assertThat(with2ShinglesEdgeNGramsIndexAnalyzer.getMaxGram(), equalTo(20));
        }

    }
}
