/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

public class IndexFieldTypeTests extends ESTestCase {

    public void testPrefixQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("ind", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("other_ind", null, createContext()));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("ind*x", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("iNd*x", null, true, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("other_ind*x", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("Other_ind*x", null, true, createContext()));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        QueryShardException e = expectThrows(
            QueryShardException.class,
            () -> assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 0, 10, null, createContext()))
        );
        assertThat(e.getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
    }

    private SearchExecutionContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
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
            parserConfig(),
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
