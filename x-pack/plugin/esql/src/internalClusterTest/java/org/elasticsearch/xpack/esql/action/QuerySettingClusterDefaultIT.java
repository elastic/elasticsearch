/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.After;

import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end for an operator-supplied query-setting default: what a cluster setting is set to has to reach query
 * execution, be overridable per query, and stop applying when it is removed.
 *
 * <p>The unit layer covers the pieces — the derivation and the per-setting fallback — but every one of them stays
 * green if {@code EsqlSession} stops reading cluster state, at which point the feature is silently dead. This is the
 * test that fails when that happens.
 */
public class QuerySettingClusterDefaultIT extends AbstractEsqlIntegTestCase {

    private static final String TIME_ZONE_KEY = "esql.query.settings.time_zone";
    private static final String UNMAPPED_FIELDS_KEY = "esql.query.settings.unmapped_fields";

    // Truncating to the day is timezone-sensitive, and this instant is chosen so the two answers fall on different
    // calendar days: just past midnight in UTC, still the previous evening at -05:00. A change that only reached the
    // response formatter and not the truncation itself would not move the date.
    private static final String TRUNCATE_TO_DAY = """
        ROW d = "2026-01-01T00:30:00Z"::datetime | EVAL truncated = DATE_TRUNC(1 day, d) | KEEP truncated""";

    private static final String UTC_RESULT = "2026-01-01T00:00:00.000Z";
    private static final String MINUS_FIVE_RESULT = "2025-12-31T00:00:00.000-05:00";

    @After
    public void clearClusterDefaults() {
        updateClusterSettings(Settings.builder().putNull(TIME_ZONE_KEY).putNull(UNMAPPED_FIELDS_KEY));
    }

    public void testClusterDefaultAppliesToQueriesThatDoNotSpecifyIt() {
        assertThat(truncatedDay(TRUNCATE_TO_DAY), equalTo(UTC_RESULT));

        setClusterTimeZone("-05:00");
        assertThat(truncatedDay(TRUNCATE_TO_DAY), equalTo(MINUS_FIVE_RESULT));

        // Dynamic: no restart, and the next query sees it.
        setClusterTimeZone(null);
        assertThat(truncatedDay(TRUNCATE_TO_DAY), equalTo(UTC_RESULT));
    }

    public void testInQuerySetOverridesTheClusterDefault() {
        setClusterTimeZone("-05:00");
        assertThat(truncatedDay("SET time_zone = \"Z\"; " + TRUNCATE_TO_DAY), equalTo(UTC_RESULT));
    }

    public void testSettingWithoutAClusterDefaultIsRejectedAsUnknown() {
        // column_metadata deliberately has no cluster key, so its would-be key must not be quietly accepted.
        var e = expectThrows(
            Exception.class,
            () -> updateClusterSettings(Settings.builder().put("esql.query.settings.column_metadata", true))
        );
        assertThat(e.getMessage(), containsString("esql.query.settings.column_metadata"));
    }

    public void testUnmappedFieldsClusterDefaultChangesQueryOutcome() {
        // A second setting, and one whose cluster default changes whether a query succeeds at all rather than just
        // what it returns. It is also SET-only on the request side, so this is the axis-independence case end to end.
        assertAcked(prepareCreate("cluster_default_unmapped").setMapping("mapped_field", "type=keyword"));
        client().prepareIndex("cluster_default_unmapped").setSource("mapped_field", "v").setRefreshPolicy(IMMEDIATE).get();

        String query = "FROM cluster_default_unmapped | KEEP mapped_field, absent_field | LIMIT 1";

        // Built-in default is DEFAULT: referencing an unmapped field is an error.
        expectThrows(VerificationException.class, () -> run(query).close());

        updateClusterSettings(Settings.builder().put(UNMAPPED_FIELDS_KEY, "NULLIFY"));
        try (EsqlQueryResponse response = run(query)) {
            List<Object> row = getValuesList(response).get(0);
            assertThat(row.get(0), equalTo("v"));
            assertThat(row.get(1), nullValue());
        }

        // Removing it restores the built-in default.
        updateClusterSettings(Settings.builder().putNull(UNMAPPED_FIELDS_KEY));
        expectThrows(VerificationException.class, () -> run(query).close());
    }

    private String truncatedDay(String query) {
        try (EsqlQueryResponse response = run(query)) {
            List<Object> row = getValuesList(response).get(0);
            return row.get(0).toString();
        }
    }

    private void setClusterTimeZone(String value) {
        Settings.Builder builder = Settings.builder();
        if (value == null) {
            builder.putNull(TIME_ZONE_KEY);
        } else {
            builder.put(TIME_ZONE_KEY, value);
        }
        updateClusterSettings(builder);
    }
}
