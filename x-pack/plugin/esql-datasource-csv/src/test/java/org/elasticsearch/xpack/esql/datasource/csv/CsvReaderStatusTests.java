/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CsvReaderStatusTests extends AbstractWireSerializingTestCase<CsvReaderStatus> {

    @Override
    protected Writeable.Reader<CsvReaderStatus> instanceReader() {
        return CsvReaderStatus::new;
    }

    @Override
    protected CsvReaderStatus createTestInstance() {
        return new CsvReaderStatus(
            randomFrom("csv", "tsv"),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected CsvReaderStatus mutateInstance(CsvReaderStatus instance) {
        return new CsvReaderStatus(
            instance.format(),
            instance.rowsEmitted(),
            instance.parseErrors(),
            instance.headerDetected(),
            randomValueOtherThan(instance.readNanos(), () -> randomNonNegativeLong())
        );
    }

    public void testToXContent() throws IOException {
        CsvReaderStatus status = new CsvReaderStatus("tsv", 100L, 3L, true, 150L);
        assertThat(
            toJson(status),
            equalTo("{\"format\":\"tsv\",\"rows_emitted\":100,\"parse_errors\":3,\"header_detected\":true,\"read_nanos\":150}")
        );
    }

    private static String toJson(CsvReaderStatus status) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
