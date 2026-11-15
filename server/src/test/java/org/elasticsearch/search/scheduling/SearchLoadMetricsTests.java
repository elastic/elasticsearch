/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SearchLoadMetricsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(IndicesModule.getNamedWriteables());

    public void testOfTheLoad() {
        QueryLoad load = new QueryLoad();
        load.charging((nanos, cpuNanos, finishNanos) -> {}).charge(300, 200, 0);
        load.addWaiting(50);
        assertThat(SearchLoadMetrics.of(load).metrics(SearchLoadMetrics.NAME), equalTo(new SearchLoadMetrics(300, 200, 50)));
    }

    /**
     * A search reports the sum of the loads of its results.
     */
    public void testMergedLoadsReportTheirSums() {
        DirectoryMetrics merged = metrics(new SearchLoadMetrics(300, 200, 50)).merge(metrics(new SearchLoadMetrics(100, 10, 5)));
        assertThat(merged.entries(), equalTo(Map.of("search_load_nanos", "400", "search_cpu_nanos", "210", "search_waiting_nanos", "55")));
    }

    /**
     * A node that doesn't know the load gets the other metrics without it.
     */
    public void testOnlyNodesThatKnowTheLoadGetIt() throws IOException {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, new StoreMetrics(42));
        builder.add(SearchLoadMetrics.NAME, new SearchLoadMetrics(300, 200, 50));
        DirectoryMetrics metrics = builder.build();

        DirectoryMetrics current = copy(metrics, TransportVersion.current());
        assertThat(current.metrics(SearchLoadMetrics.NAME), equalTo(new SearchLoadMetrics(300, 200, 50)));

        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("search_load_report"));
        DirectoryMetrics older = copy(metrics, before);
        assertNull(older.metrics(SearchLoadMetrics.NAME));
        assertThat(older.metrics(StoreMetrics.NAME), equalTo(new StoreMetrics(42)));
    }

    private static DirectoryMetrics metrics(SearchLoadMetrics load) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(SearchLoadMetrics.NAME, load);
        return builder.build();
    }

    private DirectoryMetrics copy(DirectoryMetrics metrics, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            metrics.writeTo(out);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setTransportVersion(version);
                return new DirectoryMetrics(in);
            }
        }
    }
}
