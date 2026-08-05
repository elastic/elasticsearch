/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class ParquetReaderStatusTests extends AbstractWireSerializingTestCase<ParquetReaderStatus> {

    @Override
    protected Writeable.Reader<ParquetReaderStatus> instanceReader() {
        return ParquetReaderStatus::new;
    }

    @Override
    protected ParquetReaderStatus createTestInstance() {
        return new ParquetReaderStatus(
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomBoolean(),
            randomList(0, 4, () -> randomAlphaOfLength(6)).stream().sorted().toList(),
            randomNonNegativeLong(),
            randomColumns()
        );
    }

    /** Renders the fragment wrapped in an object, mirroring how the operator status emits format_reader. */
    private static String toJson(ParquetReaderStatus status) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static Map<String, PerColumnStatus> randomColumns() {
        Map<String, PerColumnStatus> columns = new TreeMap<>();
        int n = between(0, 3);
        for (int i = 0; i < n; i++) {
            columns.put(
                randomAlphaOfLength(8),
                new PerColumnStatus(randomFrom(PerColumnStatus.MATERIALIZATION_EAGER, PerColumnStatus.MATERIALIZATION_LATE, null))
            );
        }
        return columns;
    }

    @Override
    protected ParquetReaderStatus mutateInstance(ParquetReaderStatus instance) {
        return new ParquetReaderStatus(
            instance.rowsEmitted(),
            instance.predicatePushdownUsed(),
            instance.footerReadNanos(),
            instance.footerSizeBytes(),
            instance.footerCacheHits(),
            instance.footerCacheMisses(),
            instance.rowGroupsInFile(),
            instance.rowGroupsTotal(),
            instance.rowGroupsKept(),
            instance.pageIndexUsed(),
            instance.rowsInKeptRowGroups(),
            instance.rowsAfterPageIndex(),
            instance.lateMaterializationEnabled(),
            instance.lateMaterializationUsed(),
            instance.predicateColumns(),
            randomValueOtherThan(instance.readNanos(), () -> randomNonNegativeLong()),
            instance.columns()
        );
    }

    /** Locks the format_reader JSON shape, including the typed nested per-column entries. */
    public void testToXContentWithColumns() throws IOException {
        ParquetReaderStatus status = new ParquetReaderStatus(
            100L,
            true,
            5L,
            6L,
            1L,
            2L,
            3L,
            4L,
            2L,
            false,
            10L,
            8L,
            false,
            false,
            List.of("host"),
            150L,
            Map.of("host", new PerColumnStatus(PerColumnStatus.MATERIALIZATION_LATE))
        );
        assertThat(
            toJson(status),
            equalTo(
                "{\"format\":\"parquet\",\"rows_emitted\":100,\"predicate_pushdown_used\":true,"
                    + "\"footer_read_nanos\":5,\"footer_size_bytes\":6,\"footer_cache_hits\":1,\"footer_cache_misses\":2,"
                    + "\"row_groups_in_file\":3,\"row_groups_total\":4,\"row_groups_kept\":2,"
                    + "\"page_index_used\":false,\"rows_in_kept_row_groups\":10,\"rows_after_page_index\":8,"
                    + "\"late_materialization_enabled\":false,\"late_materialization_used\":false,"
                    + "\"predicate_columns\":[\"host\"],\"read_nanos\":150,"
                    + "\"columns\":{\"host\":{\"materialization\":\"late\"}}}"
            )
        );
    }
}
