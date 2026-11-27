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

public class SearchMemoryMetricsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(IndicesModule.getNamedWriteables());

    /**
     * A search reports the highest peak of its results, whichever comes first.
     */
    public void testMergedPeaksReportTheHighest() {
        long low = randomLongBetween(0, 1000);
        long high = randomLongBetween(low, 2000);
        DirectoryMetrics lowFirst = metrics(low).merge(metrics(high));
        DirectoryMetrics highFirst = metrics(high).merge(metrics(low));
        assertThat(lowFirst.entries(), equalTo(Map.of("search_peak_memory_bytes", Long.toString(high))));
        assertThat(highFirst.entries(), equalTo(Map.of("search_peak_memory_bytes", Long.toString(high))));
    }

    /**
     * A node that doesn't know the peak gets the other metrics without it.
     */
    public void testOnlyNodesThatKnowThePeakGetIt() throws IOException {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, new StoreMetrics(42));
        builder.add(SearchMemoryMetrics.NAME, new SearchMemoryMetrics(4096));
        DirectoryMetrics metrics = builder.build();

        DirectoryMetrics current = copy(metrics, TransportVersion.current());
        assertThat(current.metrics(SearchMemoryMetrics.NAME), equalTo(new SearchMemoryMetrics(4096)));

        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("search_peak_memory"));
        DirectoryMetrics older = copy(metrics, before);
        assertNull(older.metrics(SearchMemoryMetrics.NAME));
        assertThat(older.metrics(StoreMetrics.NAME), equalTo(new StoreMetrics(42)));
    }

    private static DirectoryMetrics metrics(long peakBytes) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(SearchMemoryMetrics.NAME, new SearchMemoryMetrics(peakBytes));
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
