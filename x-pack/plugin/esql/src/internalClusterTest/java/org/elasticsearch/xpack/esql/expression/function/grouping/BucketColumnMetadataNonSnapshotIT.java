/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.Build;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Counterpart to {@link BucketColumnMetadataIT} that runs only in release builds, where the
 * {@link EsqlCapabilities.Cap#COLUMN_METADATA_BUCKET_V2} capability is disabled. Verifies that the bucket
 * metadata path is fully gated: no {@code _meta} surfaces on a {@code BUCKET} grouping column.
 */
public class BucketColumnMetadataNonSnapshotIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireNonSnapshot() {
        assumeFalse("this test only runs in release builds", Build.current().isSnapshot());
    }

    public void testNoBucketMetadataInReleaseBuild() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates | STATS c=COUNT(*) BY bucket=BUCKET(date, 1 month)
            """))) {
            assertThat(response.columns().get(1).name(), equalTo("bucket"));
            assertThat(response.columns().get(1).meta(), nullValue());
        }
    }

    public void testColumnMetadataSettingRejectedInReleaseBuild() {
        for (boolean value : new boolean[] { true, false }) {
            var request = syncEsqlQueryRequest("SET column_metadata=" + value + "; ROW x = 1");
            Exception e = expectThrows(Exception.class, () -> { try (var r = run(request)) {} });
            assertThat(e.getMessage(), containsString("Setting [column_metadata] is only available in snapshot builds"));
        }
    }
}
