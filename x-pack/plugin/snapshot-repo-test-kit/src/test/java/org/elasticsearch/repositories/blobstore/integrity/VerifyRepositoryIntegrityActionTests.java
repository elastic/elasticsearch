/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.integrity;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class VerifyRepositoryIntegrityActionTests extends ESTestCase {

    public void testStatusToXContent() throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.humanReadable(true);
            new VerifyRepositoryIntegrityAction.Status(
                "repo",
                1,
                "uuid",
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                ByteSizeValue.ofKb(4000).getBytes(),
                TimeValue.timeValueSeconds(350).nanos(),
                9,
                "results"
            ).toXContent(builder, new ToXContent.MapParams(Map.of("human", "true")));
            assertThat(BytesReference.bytes(builder).utf8ToString(), equalTo("""
                {"repository":{"name":"repo","uuid":"uuid","generation":1},"snapshots":{"verified":3,"total":2},\
                "indices":{"verified":5,"total":4},"index_snapshots":{"verified":7,"total":6},\
                "blobs":{"verified":8,"verified_size":"3.9mb","verified_size_in_bytes":4096000,\
                "throttled_time":"5.8m","throttled_time_in_millis":350000},"anomalies":9,"results_index":"results"}"""));
        }

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.humanReadable(true);
            new VerifyRepositoryIntegrityAction.Status(
                "repo",
                1,
                "uuid",
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                ByteSizeValue.ofKb(4000).getBytes(),
                0,
                9,
                "results"
            ).toXContent(builder, new ToXContent.MapParams(Map.of("human", "true")));
            assertThat(BytesReference.bytes(builder).utf8ToString(), equalTo("""
                {"repository":{"name":"repo","uuid":"uuid","generation":1},"snapshots":{"verified":3,"total":2},\
                "indices":{"verified":5,"total":4},"index_snapshots":{"verified":7,"total":6},\
                "blobs":{"verified":8},"anomalies":9,"results_index":"results"}"""));
        }
    }

    // TODO wire serialization tests
}
