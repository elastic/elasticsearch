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
 * Reruns the {@link EsqlSecurityIT} field-level-security suite with the FLS indices in {@code logsdb_columnar} mode. The
 * {@code logsdb} counterpart is {@link EsqlSecurityLogsdbIT}; see its javadoc for why the two modes are separate single-mode classes.
 *
 * <p>Columnar disables auto-text, so a dynamically-mapped string is a {@code keyword} rather than {@code text}; the overrides below
 * assert that per-mode. Columnar also rejects {@code dynamic: false}, so the indices that rely on it are created in standard mode.
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
     * Columnar rejects {@code dynamic: false}, so the indices that rely on it are created in standard mode, keeping their unmapped
     * fields in {@code _source} as the base expectations assume.
     */
    @Override
    protected Settings dynamicFalseIndexSettings() {
        return Settings.EMPTY;
    }

    @Override
    protected String dropModeSpecificColumns() {
        return " | DROP @timestamp";
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
     * Columnar disables auto-text, so {@code partial} is a {@code keyword}; the sort order and the winning row are the same as the
     * base class.
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
                .entry("values", List.of(List.of("engineering20.0", 20.0)))
        );
    }

    @Override
    public void testFieldLevelSecurityWithRuntimeMappingTargetingConstantKeyword() {
        assumeTrue("mapping-level runtime fields are not supported in logsdb_columnar mode", false);
    }
}
