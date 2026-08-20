/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Reruns the entire {@link EsqlSecurityIT} field-level-security suite with the FLS indices in {@code logsdb} mode, proving that ESQL
 * FLS enforcement is identical whether {@code _source} is stored or reconstructed from doc values / {@code _ignored_source}.
 *
 * <p>The {@code logsdb_columnar} counterpart lives in {@link EsqlSecurityLogsdbColumnarIT}; the two are separate single-mode classes
 * rather than one {@code @ParametersFactory} because behavior differs between the two index modes - notably, {@code logsdb_columnar}
 * drops {@code dynamic:false} unmapped fields at index time.
 *
 * <p>Two tracked FLS x synthetic-source bugs are disabled for this class via {@code @AwaitsFix} overrides at the bottom of the file:
 * <ol>
 *   <li>FLS drops the keyword synthetic-source delegate so a granted text field reconstructs to null (elastic/security#6714).</li>
 *   <li>{@code except:_source} fails to strip values reconstructed from {@code _ignored_source} (elastic/security#13332).</li>
 * </ol>
 */
public class EsqlSecurityLogsdbIT extends EsqlSecurityIT {

    @Override
    protected Settings indexSettings() {
        return Settings.builder().put("index.mode", "logsdb").build();
    }

    /**
     * Disables the data-stream {@code @timestamp} metadata field that logsdb enables by default, so the shared timestamp-less test
     * documents index unchanged.
     */
    @Override
    protected String mappingPrefix() {
        return "\"_data_stream_timestamp\":{\"enabled\":false},";
    }

    /**
     * Override this test because LOAD_ALL surfaces an extra empty {@code @timestamp} column. We must drop this field in order to line up
     * the columns with the base run.
     */
    @Override
    public void testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoadAll() throws Exception {
        // drop timestamp as described in the javadoc
        String query = "SET unmapped_fields=\"LOAD_ALL\"; FROM " + INDEX_PARTIAL_MAPPING + " | SORT salary | LIMIT 10 | DROP @timestamp";

        Response adminResp = runESQLCommand("test-admin", query);
        assertOK(adminResp);
        assertMap(
            entityAsMap(adminResp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "value").entry("type", "double"),
                        matchesMap().entry("name", "salary").entry("type", "keyword"),
                        matchesMap().entry("name", "hire_date").entry("type", "keyword"),
                        matchesMap().entry("name", "ip_addr").entry("type", "keyword"),
                        matchesMap().entry("name", "org").entry("type", "keyword")
                    )
                )
                .entry(
                    "values",
                    List.of(
                        List.of(10.0, "100000", "2024-01-01", "10.0.0.1", "sales"),
                        List.of(20.0, "200000", "2023-06-15", "10.0.0.2", "engineering")
                    )
                )
        );

        Response restrictedResp = runESQLCommand("fls_deny_value_org_user", query);
        assertOK(restrictedResp);
        assertMap(
            entityAsMap(restrictedResp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "salary").entry("type", "keyword"),
                        matchesMap().entry("name", "hire_date").entry("type", "keyword"),
                        matchesMap().entry("name", "ip_addr").entry("type", "keyword")
                    )
                )
                .entry("values", List.of(List.of("100000", "2024-01-01", "10.0.0.1"), List.of("200000", "2023-06-15", "10.0.0.2")))
        );
    }

    // FLS drops the keyword synthetic-source delegate so a granted text field reconstructs to null

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecurityAllow() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecurityAllowPartial() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecurityPartiallyUnmappedLoad() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecurityPartiallyUnmappedNullify() throws Exception {}

    // except:_source fails to strip values reconstructed from _ignored_source

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecuritySourceDisabledMultiIndex() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecuritySourceDisabledMultiIndexPartialMappingNonKeyword() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoad() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "TODO")
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoadAndCast() throws Exception {}
}
