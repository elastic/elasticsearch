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
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class IndexModeFieldTypeTests extends ConstantFieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = getMappedFieldType();
        for (IndexMode mode : IndexMode.values()) {
            SearchExecutionContext context = createContext(mode);
            for (IndexMode other : IndexMode.values()) {
                Query query = ft.termQuery(other.getName(), context);
                if (other.equals(mode)) {
                    assertEquals(new MatchAllDocsQuery(), query);
                } else {
                    assertEquals(new MatchNoDocsQuery(), query);
                }
            }
        }
    }

    public void testWildcardQuery() {
        MappedFieldType ft = getMappedFieldType();

        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("stand*", null, createContext(IndexMode.STANDARD)));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("stand*", null, createContext(IndexMode.TIME_SERIES)));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("stand*", null, createContext(IndexMode.LOGSDB)));

        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("time*", null, createContext(IndexMode.STANDARD)));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("time*", null, createContext(IndexMode.TIME_SERIES)));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("time*", null, createContext(IndexMode.LOGSDB)));

        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("logs*", null, createContext(IndexMode.STANDARD)));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("logs*", null, createContext(IndexMode.TIME_SERIES)));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("logs*", null, createContext(IndexMode.LOGSDB)));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return IndexModeFieldMapper.IndexModeFieldType.INSTANCE;
    }

    private SearchExecutionContext createContext(IndexMode mode) {
        Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (mode != null) {
            settings.put(IndexSettings.MODE.getKey(), mode);
        }
        if (mode == IndexMode.TIME_SERIES) {
            settings.putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("a,b,c"));
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings.build());

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
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
            Collections.emptyMap(),
            MapperMetrics.NOOP
        );
    }
}
