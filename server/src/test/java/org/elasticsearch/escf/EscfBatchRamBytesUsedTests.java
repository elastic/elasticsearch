/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EscfBatchRamBytesUsedTests extends ESTestCase {

    public void testBuiltBatchKeepsItsSizeOnceSerialized() {
        try (EscfBatch batch = batch(between(1, 500))) {
            long built = batch.ramBytesUsed();
            assertThat(built, greaterThan(EscfBatch.BASE_RAM_BYTES_USED));
            batch.data();
            assertThat(batch.ramBytesUsed(), equalTo(built));
        }
    }

    public void testReceivedBatchIsItsSerializedSize() {
        try (EscfBatch batch = batch(between(1, 500))) {
            BytesReference data = batch.data();
            try (EscfBatch received = EscfBatch.parse(data, () -> {})) {
                assertThat(received.ramBytesUsed(), equalTo(data.length() + EscfBatch.BASE_RAM_BYTES_USED));
            }
        }
    }

    private static EscfBatch batch(int rows) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder()) {
            for (int i = 0; i < rows; i++) {
                byte[] value = randomAlphaOfLength(between(1, 20)).getBytes(StandardCharsets.UTF_8);
                EscfRowBuffer row = builder.beginRow();
                row.longField("long", i);
                row.stringField("string", new XContentString.UTF8Bytes(value, 0, value.length));
                row.finishRow();
                builder.commit(0);
            }
            return builder.buildPartition(0);
        }
    }
}
