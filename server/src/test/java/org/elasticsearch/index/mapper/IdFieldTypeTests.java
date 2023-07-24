/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

public class IdFieldTypeTests extends ESTestCase {

    public void testRangeQuery() {
        MappedFieldType ft = randomBoolean()
            ? new ProvidedIdFieldMapper.IdFieldType(() -> false)
            : new TsidExtractingIdFieldMapper.IdFieldType();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null)
        );
        assertEquals("Field [_id] of type [_id] does not support range queries", e.getMessage());
    }

    public void testTermsQuery() {
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);

        Settings.Builder indexSettings = indexSettings(Version.CURRENT, 1, 0).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        );
        if (randomBoolean()) {
            indexSettings.put(IndexSettings.MODE.getKey(), "time_series");
            indexSettings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo");
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);
        Mockito.when(context.indexVersionCreated()).thenReturn(IndexVersion.current());
        MappedFieldType ft = new ProvidedIdFieldMapper.IdFieldType(() -> false);
        Query query = ft.termQuery("id", context);
        assertEquals(new TermInSetQuery("_id", Uid.encodeId("id")), query);
    }

    public void testIsAggregatable() {
        MappedFieldType ft = new ProvidedIdFieldMapper.IdFieldType(() -> false);
        assertFalse(ft.isAggregatable());

        ft = new ProvidedIdFieldMapper.IdFieldType(() -> true);
        assertTrue(ft.isAggregatable());

        ft = new TsidExtractingIdFieldMapper.IdFieldType();
        assertFalse(ft.isAggregatable());
    }
}
