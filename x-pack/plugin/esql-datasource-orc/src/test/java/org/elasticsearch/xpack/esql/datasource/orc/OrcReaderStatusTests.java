/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class OrcReaderStatusTests extends AbstractWireSerializingTestCase<OrcReaderStatus> {

    @Override
    protected Writeable.Reader<OrcReaderStatus> instanceReader() {
        return OrcReaderStatus::new;
    }

    @Override
    protected OrcReaderStatus createTestInstance() {
        return new OrcReaderStatus(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomList(0, 4, () -> randomAlphaOfLength(6)).stream().sorted().toList(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected OrcReaderStatus mutateInstance(OrcReaderStatus instance) {
        return new OrcReaderStatus(
            instance.rowsEmitted(),
            instance.footerReadNanos(),
            instance.footerSizeBytes(),
            instance.footerCacheHits(),
            instance.footerCacheMisses(),
            instance.stripesInFile(),
            instance.stripesTotal(),
            instance.predicatePushdownUsed(),
            instance.predicateColumns(),
            instance.columnsProjected(),
            instance.columnsTotal(),
            randomValueOtherThan(instance.readNanos(), () -> randomNonNegativeLong())
        );
    }

    public void testToXContent() throws IOException {
        OrcReaderStatus status = new OrcReaderStatus(100L, 5L, 6L, 1L, 2L, 3L, 4L, true, List.of("host"), 7L, 9L, 150L);
        assertThat(
            toJson(status),
            equalTo(
                "{\"format\":\"orc\",\"rows_emitted\":100,\"footer_read_nanos\":5,\"footer_size_bytes\":6,"
                    + "\"footer_cache_hits\":1,\"footer_cache_misses\":2,\"stripes_in_file\":3,\"stripes_total\":4,"
                    + "\"predicate_pushdown_used\":true,\"predicate_columns\":[\"host\"],"
                    + "\"columns_projected\":7,\"columns_total\":9,\"read_nanos\":150}"
            )
        );
    }

    private static String toJson(OrcReaderStatus status) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
