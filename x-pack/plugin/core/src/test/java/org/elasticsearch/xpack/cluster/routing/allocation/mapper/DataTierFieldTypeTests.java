/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

public class DataTierFieldTypeTests extends MapperServiceTestCase {

    public void testPrefixQuery() throws IOException {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("data_w", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("noSuchRole", null, createContext()));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("data_w*", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("data_warm", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("Data_Warm", null, true, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("Data_Warm", null, false, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("noSuchRole", null, createContext()));
    }

    public void testTermQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("data_warm", createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("data_hot", createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("noSuchRole", createContext()));
    }

    public void testTermsQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("data_warm"), createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Arrays.asList("data_cold", "data_frozen"), createContext()));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        QueryShardException e = expectThrows(
            QueryShardException.class,
            () -> assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 0, 10, null, createContext()))
        );
        assertThat(e.getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
    }

    private SearchExecutionContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    // Tier can be an ordered list of preferences - starting with primary and followed by fallbacks.
                    .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_hot")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            System::currentTimeMillis,
            null,
            indexNameMatcher,
            () -> true,
            null,
            emptyMap()
        );
    }
}
