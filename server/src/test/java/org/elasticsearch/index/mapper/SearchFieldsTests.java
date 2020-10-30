/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchFieldsTests extends ESTestCase {

    public void testFailIfFieldMappingNotFound() {
        SearchFields searchFields = createSearchFields(IndexMetadata.INDEX_UUID_NA_VALUE);
        searchFields.setAllowUnmappedFields(false);
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("text");
        MappedFieldType result = searchFields.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> searchFields.failIfFieldMappingNotFound("name", null));
        assertEquals("No field mapping can be found for the field with name [name]", e.getMessage());

        searchFields.setAllowUnmappedFields(true);
        result = searchFields.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = searchFields.failIfFieldMappingNotFound("name", null);
        assertThat(result, nullValue());

        searchFields.setAllowUnmappedFields(false);
        searchFields.setMapUnmappedFieldAsString(true);
        result = searchFields.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = searchFields.failIfFieldMappingNotFound("name", null);
        assertThat(result, notNullValue());
        assertThat(result, instanceOf(TextFieldMapper.TextFieldType.class));
        assertThat(result.name(), equalTo("name"));
    }

    public void testBuildAnonymousFieldType() {
        SearchFields searchFields = createSearchFields("uuid");
        assertThat(searchFields.buildAnonymousFieldType("keyword"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertThat(searchFields.buildAnonymousFieldType("long"), instanceOf(NumberFieldMapper.NumberFieldType.class));
    }

    private static SearchFields createSearchFields(String indexUuid) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder("index");
        indexMetadataBuilder.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
        );
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
            Collections.singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, null)),
            Collections.emptyMap(), Collections.emptyMap()
        );
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.index()).thenReturn(indexMetadata.getIndex());
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        Map<String, Mapper.TypeParser> typeParserMap = IndicesModule.getMappers(Collections.emptyList());
        Mapper.TypeParser.ParserContext parserContext = new Mapper.TypeParser.ParserContext(name -> null, typeParserMap::get,
            Version.CURRENT, () -> null, null, null, indexAnalyzers, indexSettings,
            () -> {
                throw new UnsupportedOperationException();
            });
        when(mapperService.parserContext()).thenReturn(parserContext);
        return new SearchFields(mapperService);
    }
}
