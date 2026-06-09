/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NdJsonReaderStatusTests extends AbstractWireSerializingTestCase<NdJsonReaderStatus> {

    @Override
    protected Writeable.Reader<NdJsonReaderStatus> instanceReader() {
        return NdJsonReaderStatus::new;
    }

    @Override
    protected NdJsonReaderStatus createTestInstance() {
        return new NdJsonReaderStatus(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected NdJsonReaderStatus mutateInstance(NdJsonReaderStatus instance) {
        return new NdJsonReaderStatus(
            instance.rowsEmitted(),
            instance.parseErrors(),
            randomValueOtherThan(instance.readNanos(), () -> randomNonNegativeLong())
        );
    }

    public void testToXContent() throws IOException {
        NdJsonReaderStatus status = new NdJsonReaderStatus(100L, 3L, 150L);
        assertThat(toJson(status), equalTo("{\"format\":\"ndjson\",\"rows_emitted\":100,\"parse_errors\":3,\"read_nanos\":150}"));
    }

    private static String toJson(NdJsonReaderStatus status) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
