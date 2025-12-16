/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;

public class DataTierFieldTypeTests extends MapperServiceTestCase {

    public void testPrefixQuery() throws IOException {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("data_w", null, createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("noSuchRole", null, createContext()));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("data_w*", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("data_warm", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("Data_Warm", null, true, createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("Data_Warm", null, false, createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("noSuchRole", null, createContext()));

        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("data_*", null, createContextWithoutSetting()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("*", null, createContextWithoutSetting()));
    }

    public void testTermQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("data_warm", createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("data_hot", createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("noSuchRole", createContext()));

        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("data_warm", createContextWithoutSetting()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("", createContextWithoutSetting()));
    }

    public void testTermsQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("data_warm"), createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("data_cold", "data_frozen"), createContext()));

        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("data_warm"), createContextWithoutSetting()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList(""), createContextWithoutSetting()));
    }

    public void testExistsQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        assertEquals(new MatchAllDocsQuery(), ft.existsQuery(createContext()));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.existsQuery(createContextWithoutSetting()));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        QueryShardException e = expectThrows(
            QueryShardException.class,
            () -> assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 0, 10, null, createContext()))
        );
        assertThat(e.getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
    }

    public void testFetchValue() throws IOException {
        MappedFieldType ft = DataTierFieldMapper.DataTierFieldType.INSTANCE;
        Source source = Source.empty(XContentType.JSON);

        List<Object> ignoredValues = new ArrayList<>();
        ValueFetcher valueFetcher = ft.valueFetcher(createContext(), null);
        assertEquals(singletonList("data_warm"), valueFetcher.fetchValues(source, -1, ignoredValues));

        ValueFetcher emptyValueFetcher = ft.valueFetcher(createContextWithoutSetting(), null);
        assertTrue(emptyValueFetcher.fetchValues(source, -1, ignoredValues).isEmpty());
    }

    private SearchExecutionContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    // Tier can be an ordered list of preferences - starting with primary and followed by fallbacks.
                    .put(DataTier.TIER_PREFERENCE, "data_warm,data_hot")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return SearchExecutionContextHelper.createSimple(indexSettings, parserConfig(), writableRegistry());
    }

    private SearchExecutionContext createContextWithoutSetting() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        return SearchExecutionContextHelper.createSimple(indexSettings, parserConfig(), writableRegistry());
    }

    @Override
    public void testFieldHasValue() {
        assertTrue(getMappedFieldType().fieldHasValue(new FieldInfos(new FieldInfo[] { getFieldInfoWithName(randomAlphaOfLength(5)) })));
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assertTrue(getMappedFieldType().fieldHasValue(FieldInfos.EMPTY));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return DataTierFieldMapper.DataTierFieldType.INSTANCE;
    }
}
