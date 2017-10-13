/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.BaseFilteredMonitoringDocTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.junit.Before;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexStatsMonitoringDocTests extends BaseFilteredMonitoringDocTestCase<IndexStatsMonitoringDoc> {


    private IndexStats indexStats;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexStats = mock(IndexStats.class);
    }

    @Override
    protected IndexStatsMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                          MonitoredSystem system, String type, String id) {
        return new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, indexStats);
    }

    @Override
    protected void assertFilteredMonitoringDoc(final IndexStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(IndexStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getIndexStats(), is(indexStats));
    }

    @Override
    protected Set<String> getExpectedXContentFilters() {
        return IndexStatsMonitoringDoc.XCONTENT_FILTERS;
    }

    public void testConstructorIndexStatsMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new IndexStatsMonitoringDoc(cluster, timestamp, interval, node, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        when(indexStats.getIndex()).thenReturn("_index");
        when(indexStats.getTotal()).thenReturn(mockCommonStats());
        when(indexStats.getPrimaries()).thenReturn(mockCommonStats());

        final IndexStatsMonitoringDoc document = new IndexStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, indexStats);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertEquals("{"
                     + "\"cluster_uuid\":\"_cluster\","
                     + "\"timestamp\":\"2017-08-09T08:18:59.402Z\","
                     + "\"interval_ms\":1506593717631,"
                     + "\"type\":\"index_stats\","
                     + "\"source_node\":{"
                       + "\"uuid\":\"_uuid\","
                       + "\"host\":\"_host\","
                       + "\"transport_address\":\"_addr\","
                       + "\"ip\":\"_ip\","
                       + "\"name\":\"_name\","
                       + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                     + "},"
                     + "\"index_stats\":{"
                       + "\"index\":\"_index\","
                       + "\"total\":{"
                         + "\"docs\":{"
                           + "\"count\":1"
                         + "},"
                         + "\"store\":{"
                           + "\"size_in_bytes\":13"
                         + "},"
                         + "\"indexing\":{"
                           + "\"index_total\":15,"
                           + "\"index_time_in_millis\":16,"
                           + "\"throttle_time_in_millis\":17"
                         + "},"
                         + "\"search\":{"
                           + "\"query_total\":18,"
                           + "\"query_time_in_millis\":19"
                         + "},"
                         + "\"merges\":{"
                           + "\"total_size_in_bytes\":4"
                         + "},"
                         + "\"refresh\":{"
                           + "\"total_time_in_millis\":14"
                         + "},"
                         + "\"query_cache\":{"
                           + "\"memory_size_in_bytes\":5,"
                           + "\"hit_count\":6,"
                           + "\"miss_count\":7,"
                           + "\"evictions\":9"
                         + "},"
                         + "\"fielddata\":{"
                           + "\"memory_size_in_bytes\":2,"
                           + "\"evictions\":3"
                         + "},"
                         + "\"segments\":{"
                           + "\"count\":20,"
                           + "\"memory_in_bytes\":21,"
                           + "\"terms_memory_in_bytes\":22,"
                           + "\"stored_fields_memory_in_bytes\":23,"
                           + "\"term_vectors_memory_in_bytes\":24,"
                           + "\"norms_memory_in_bytes\":25,"
                           + "\"points_memory_in_bytes\":26,"
                           + "\"doc_values_memory_in_bytes\":27,"
                           + "\"index_writer_memory_in_bytes\":28,"
                           + "\"version_map_memory_in_bytes\":29,"
                           + "\"fixed_bit_set_memory_in_bytes\":30"
                         + "},"
                         + "\"request_cache\":{"
                           + "\"memory_size_in_bytes\":9,"
                           + "\"evictions\":10,"
                           + "\"hit_count\":11,"
                           + "\"miss_count\":12"
                         + "}"
                       + "},"
                       + "\"primaries\":{"
                         + "\"docs\":{"
                           + "\"count\":1"
                         + "},"
                         + "\"store\":{"
                           + "\"size_in_bytes\":13"
                         + "},"
                         + "\"indexing\":{"
                           + "\"index_total\":15,"
                           + "\"index_time_in_millis\":16,"
                           + "\"throttle_time_in_millis\":17"
                         + "},"
                         + "\"search\":{"
                           + "\"query_total\":18,"
                           + "\"query_time_in_millis\":19"
                         + "},"
                         + "\"merges\":{"
                           + "\"total_size_in_bytes\":4"
                         + "},"
                         + "\"refresh\":{"
                           + "\"total_time_in_millis\":14"
                         + "},"
                         + "\"query_cache\":{"
                           + "\"memory_size_in_bytes\":5,"
                           + "\"hit_count\":6,"
                           + "\"miss_count\":7,"
                           + "\"evictions\":9"
                         + "},"
                         + "\"fielddata\":{"
                           + "\"memory_size_in_bytes\":2,"
                           + "\"evictions\":3"
                         + "},"
                         + "\"segments\":{"
                           + "\"count\":20,"
                           + "\"memory_in_bytes\":21,"
                           + "\"terms_memory_in_bytes\":22,"
                           + "\"stored_fields_memory_in_bytes\":23,"
                           + "\"term_vectors_memory_in_bytes\":24,"
                           + "\"norms_memory_in_bytes\":25,"
                           + "\"points_memory_in_bytes\":26,"
                           + "\"doc_values_memory_in_bytes\":27,"
                           + "\"index_writer_memory_in_bytes\":28,"
                           + "\"version_map_memory_in_bytes\":29,"
                           + "\"fixed_bit_set_memory_in_bytes\":30"
                         + "},"
                         + "\"request_cache\":{"
                           + "\"memory_size_in_bytes\":9,"
                           + "\"evictions\":10,"
                           + "\"hit_count\":11,"
                           + "\"miss_count\":12"
                        + "}"
                        + "}"
                      + "}"
                + "}", xContent.utf8ToString());
    }

    public void testToXContentWithNullStats() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        when(indexStats.getIndex()).thenReturn("_index");
        when(indexStats.getTotal()).thenReturn(null);
        when(indexStats.getPrimaries()).thenReturn(null);

        final IndexStatsMonitoringDoc document = new IndexStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, indexStats);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertEquals("{"
                     + "\"cluster_uuid\":\"_cluster\","
                     + "\"timestamp\":\"2017-08-09T08:18:59.402Z\","
                     + "\"interval_ms\":1506593717631,"
                     + "\"type\":\"index_stats\","
                     + "\"source_node\":{"
                       + "\"uuid\":\"_uuid\","
                       + "\"host\":\"_host\","
                       + "\"transport_address\":\"_addr\","
                       + "\"ip\":\"_ip\","
                       + "\"name\":\"_name\","
                       + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                     + "},"
                     + "\"index_stats\":{"
                       + "\"index\":\"_index\""
                      + "}"
                + "}", xContent.utf8ToString());
    }

    private static CommonStats mockCommonStats() {
        // This value is used in constructors of various stats objects,
        // when the value is not printed out in the final XContent.
        final long no = -1;

        // This value is used in constructors of various stats objects,
        // when the value is printed out in the XContent. Must be
        // incremented for each usage.
        long iota = 0L;

        final CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getDocs().add(new DocsStats(++iota, no));
        commonStats.getFieldData().add(new FieldDataStats(++iota, ++iota, null));
        commonStats.getMerge().add(no, no, no, ++iota, no, no, no, no, no, no);
        commonStats.getQueryCache().add(new QueryCacheStats(++iota, ++iota, ++iota, ++iota, no));
        commonStats.getRequestCache().add(new RequestCacheStats(++iota, ++iota, ++iota, ++iota));
        commonStats.getStore().add(new StoreStats(++iota));
        commonStats.getRefresh().add(new RefreshStats(no, ++iota, (int) no));

        final IndexingStats.Stats indexingStats = new IndexingStats.Stats(++iota, ++iota, no, no, no, no, no, no, false, ++iota);
        commonStats.getIndexing().add(new IndexingStats(indexingStats, null));

        final SearchStats.Stats searchStats = new SearchStats.Stats(++iota, ++iota, no, no, no, no, no, no, no, no, no, no);
        commonStats.getSearch().add(new SearchStats(searchStats, no, null));

        final SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(++iota, ++iota);
        segmentsStats.addTermsMemoryInBytes(++iota);
        segmentsStats.addStoredFieldsMemoryInBytes(++iota);
        segmentsStats.addTermVectorsMemoryInBytes(++iota);
        segmentsStats.addNormsMemoryInBytes(++iota);
        segmentsStats.addPointsMemoryInBytes(++iota);
        segmentsStats.addDocValuesMemoryInBytes(++iota);
        segmentsStats.addIndexWriterMemoryInBytes(++iota);
        segmentsStats.addVersionMapMemoryInBytes(++iota);
        segmentsStats.addBitsetMemoryInBytes(++iota);
        commonStats.getSegments().add(segmentsStats);

        return commonStats;
    }
}
