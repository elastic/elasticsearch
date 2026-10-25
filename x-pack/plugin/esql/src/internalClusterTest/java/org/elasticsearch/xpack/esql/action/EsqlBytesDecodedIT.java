/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End to end coverage for {@code bytes_decoded}: a query over values of a known length reports the volume those
 * values spell, and reports it above what the column they came from cost to read.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlBytesDecodedIT extends AbstractEsqlIntegTestCase {

    private static final int DOCS = 2000;
    private static final int VALUE_LENGTH = 100;

    public void testAValueReadReportsWhatItDecoded() {
        final String index = "esql_bytes_decoded";
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("body", "type=keyword")
        );
        // Every value is the same length, so the volume is exactly known, and all of them repeat one phrase, so the
        // column they are read from is far smaller than they are.
        final String phrase = "the quick brown fox jumps over the lazy dog ";
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < DOCS; doc++) {
            final String value = phrase.repeat(VALUE_LENGTH / phrase.length() + 1).substring(0, VALUE_LENGTH - 6) + String.format(
                Locale.ROOT,
                "%06d",
                doc
            );
            assertEquals(VALUE_LENGTH, value.length());
            bulk.add(client().prepareIndex(index).setSource("body", value));
        }
        bulk.get();

        // Grouping by the value reads every one of them: a count alone would read no values at all.
        final EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + index + " | STATS rows = COUNT(*) BY body | STATS groups = COUNT(*)"
        );
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            assertThat(response.profile(), notNullValue());

            long bytesDecoded = 0;
            long bytesRead = 0;
            final StringBuilder reported = new StringBuilder("\n");
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof ValuesSourceReaderOperatorStatus status) {
                        reported.append("    ").append(operator.operator()).append(' ').append(status).append('\n');
                        bytesDecoded += status.bytesDecoded();
                        bytesRead += status.bytesRead();
                    }
                }
            }
            assertThat("every value read is counted" + reported, bytesDecoded, greaterThanOrEqualTo((long) DOCS * VALUE_LENGTH));
            assertThat("the values spell more than the column cost to read" + reported, bytesDecoded, greaterThan(bytesRead));
        }
    }
}
