/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Reruns the {@link EsqlSecurityIT} field-level-security suite with the FLS indices in {@code logsdb_columnar} mode. The
 * {@code logsdb} counterpart is {@link EsqlSecurityLogsdbIT}; see its javadoc for why the two modes are separate single-mode classes.
 *
 * <p>Columnar has two documented design differences that the overrides below assert per-mode:
 * <ol>
 *   <li>It disables auto-text, so a dynamically-mapped string is a {@code keyword} rather than {@code text}.</li>
 *   <li>It drops {@code dynamic:false} unmapped fields at index time, so they load as null for every user.</li>
 * </ol>
 */
public class EsqlSecurityLogsdbColumnarIT extends EsqlSecurityIT {

    @Override
    protected Settings indexSettings() {
        return Settings.builder().put("index.mode", "logsdb_columnar").build();
    }

    /**
     * Disables the data-stream {@code @timestamp} metadata field that logsdb_columnar enables by default, so the shared
     * timestamp-less test documents index unchanged.
     */
    @Override
    protected String mappingPrefix() {
        return "\"_data_stream_timestamp\":{\"enabled\":false},";
    }

    /**
     * Columnar disables auto-text, so the dynamically-mapped {@code partial} string is a {@code keyword} rather than {@code text};
     * the value is unchanged.
     */
    @Override
    public void testFieldLevelSecurityAllow() throws Exception {
        Response resp = runESQLCommand("fls_user", "FROM index* | SORT value | LIMIT 1");
        assertOK(resp);
        assertMap(
            entityAsMap(resp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "partial").entry("type", "keyword"),
                        matchesMap().entry("name", "value").entry("type", "double")
                    )
                )
                .entry("values", List.of(List.of("sales10.0", 10.0)))
        );
    }

    /**
     * Columnar types {@code partial} as {@code keyword}; sorting a keyword rather than a text field also flips which row {@code LIMIT 1}
     * keeps.
     */
    @Override
    public void testFieldLevelSecurityAllowPartial() throws Exception {
        Response resp = runESQLCommand("fls_user", "FROM index* | SORT partial | LIMIT 1");
        assertOK(resp);
        assertMap(
            entityAsMap(resp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "partial").entry("type", "keyword"),
                        matchesMap().entry("name", "value").entry("type", "double")
                    )
                )
                .entry("values", List.of(List.of("sales10.0", 10.0)))
        );
    }

    /**
     * Columnar drops the {@code dynamic:false} unmapped fields at index time, so they load as null for every user - including the
     * admin, who sees real values under standard/logsdb source. The FLS-restricted users already expect null, so only the admin
     * response diverges from the base assertion.
     */
    @Override
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoad() throws Exception {
        String query = "SET unmapped_fields=\"load\"; FROM "
            + INDEX_PARTIAL_MAPPING
            + " | KEEP value, org, salary, hire_date, ip_addr | SORT value | LIMIT 10";
        var expectedColumns = List.of(
            matchesMap().entry("name", "value").entry("type", "double"),
            matchesMap().entry("name", "org").entry("type", "keyword"),
            matchesMap().entry("name", "salary").entry("type", "keyword"),
            matchesMap().entry("name", "hire_date").entry("type", "keyword"),
            matchesMap().entry("name", "ip_addr").entry("type", "keyword")
        );

        // Admin: the unmapped fields were dropped at index time, so they come back null despite the JSON having contained values.
        Response adminResp = runESQLCommand("test-admin", query);
        assertOK(adminResp);
        assertMap(
            entityAsMap(adminResp),
            matchesMap().extraOk()
                .entry("columns", expectedColumns)
                .entry("values", List.of(Arrays.asList(10.0, null, null, null, null), Arrays.asList(20.0, null, null, null, null)))
        );

        Response noSourceResp = runESQLCommand("fls_partial_no_source_user", query);
        assertOK(noSourceResp);
        assertMap(
            entityAsMap(noSourceResp),
            matchesMap().extraOk()
                .entry("columns", expectedColumns)
                .entry("values", List.of(Arrays.asList(10.0, null, null, null, null), Arrays.asList(20.0, null, null, null, null)))
        );

        Response noSourceNoValueResp = runESQLCommand("fls_no_source_no_value_user", query);
        assertOK(noSourceNoValueResp);
        assertMap(
            entityAsMap(noSourceNoValueResp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "value").entry("type", "keyword"),
                        matchesMap().entry("name", "org").entry("type", "keyword"),
                        matchesMap().entry("name", "salary").entry("type", "keyword"),
                        matchesMap().entry("name", "hire_date").entry("type", "keyword"),
                        matchesMap().entry("name", "ip_addr").entry("type", "keyword")
                    )
                )
                .entry("values", List.of(Arrays.asList(null, null, null, null, null), Arrays.asList(null, null, null, null, null)))
        );
    }

    /**
     * Cast variant of {@link #testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoad}: columnar drops the {@code dynamic:false}
     * unmapped fields at index time, so even the admin's cast columns come back null. The restricted user already expects null.
     */
    @Override
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoadAndCast() throws Exception {
        String query = "SET unmapped_fields=\"load\"; FROM "
            + INDEX_PARTIAL_MAPPING
            + " | EVAL salary = salary::long, hire_date = hire_date::date, ip_addr = ip_addr::ip "
            + "| KEEP value, salary, hire_date, ip_addr | SORT value | LIMIT 10";
        var expectedColumns = List.of(
            matchesMap().entry("name", "value").entry("type", "double"),
            matchesMap().entry("name", "salary").entry("type", "long"),
            matchesMap().entry("name", "hire_date").entry("type", "date"),
            matchesMap().entry("name", "ip_addr").entry("type", "ip")
        );

        Response adminResp = runESQLCommand("test-admin", query);
        assertOK(adminResp);
        assertMap(
            entityAsMap(adminResp),
            matchesMap().extraOk()
                .entry("columns", expectedColumns)
                .entry("values", List.of(Arrays.asList(10.0, null, null, null), Arrays.asList(20.0, null, null, null)))
        );

        Response restrictedResp = runESQLCommand("fls_partial_no_source_user", query);
        assertOK(restrictedResp);
        assertMap(
            entityAsMap(restrictedResp),
            matchesMap().extraOk()
                .entry("columns", expectedColumns)
                .entry("values", List.of(Arrays.asList(10.0, null, null, null), Arrays.asList(20.0, null, null, null)))
        );
    }

    /**
     * Columnar drops the {@code dynamic:false} unmapped {@code org} at index time, so the admin sees null where standard/logsdb
     * source yields "sales"/"engineering". The restricted user already expects null for both the denied {@code value} and {@code org}.
     */
    @Override
    public void testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoad() throws Exception {
        String query = "SET unmapped_fields=\"load\"; FROM " + INDEX_PARTIAL_MAPPING + " | KEEP value, org | SORT value | LIMIT 10";

        Response adminResp = runESQLCommand("test-admin", query);
        assertOK(adminResp);
        assertMap(
            entityAsMap(adminResp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "value").entry("type", "double"),
                        matchesMap().entry("name", "org").entry("type", "keyword")
                    )
                )
                .entry("values", List.of(Arrays.asList(10.0, null), Arrays.asList(20.0, null)))
        );

        Response restrictedResp = runESQLCommand("fls_deny_value_org_user", query);
        assertOK(restrictedResp);
        assertMap(
            entityAsMap(restrictedResp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "value").entry("type", "keyword"),
                        matchesMap().entry("name", "org").entry("type", "keyword")
                    )
                )
                .entry("values", List.of(Arrays.asList(null, null), Arrays.asList(null, null)))
        );
    }

    /**
     * Columnar drops the {@code dynamic:false} unmapped fields at index time, so {@code LOAD_ALL} only surfaces the mapped
     * {@code value} plus the {@code salary} that {@code SORT} references (as null); the default {@code @timestamp} column is dropped.
     * The FLS-denied {@code value} and {@code org} still never appear for the restricted user.
     */
    @Override
    public void testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoadAll() throws Exception {
        assumeTrue(
            "Requires unmapped_fields=LOAD_ALL support",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL.capabilityName()))
        );
        String query = "SET unmapped_fields=\"LOAD_ALL\"; FROM " + INDEX_PARTIAL_MAPPING + " | SORT salary | LIMIT 10 | DROP @timestamp";

        // SORT salary is a no-op on all-null values, so the two admin rows come back in an unspecified order.
        Response adminResp = runESQLCommand("test-admin", query);
        assertOK(adminResp);
        Map<String, Object> adminMap = entityAsMap(adminResp);
        assertMap(
            adminMap,
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "value").entry("type", "double"),
                        matchesMap().entry("name", "salary").entry("type", "keyword")
                    )
                )
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> adminValues = (List<List<Object>>) adminMap.get("values");
        assertThat(adminValues, containsInAnyOrder(Arrays.asList(10.0, null), Arrays.asList(20.0, null)));

        Response restrictedResp = runESQLCommand("fls_deny_value_org_user", query);
        assertOK(restrictedResp);
        assertMap(
            entityAsMap(restrictedResp),
            matchesMap().extraOk()
                .entry("columns", List.of(matchesMap().entry("name", "salary").entry("type", "keyword")))
                .entry("values", List.of(Arrays.asList((Object) null), Arrays.asList((Object) null)))
        );
    }
}
