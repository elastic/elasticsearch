/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.settings.Settings;

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
     * LOAD_ALL surfaces an extra empty {@code @timestamp} column in this mode, which has to be dropped for the columns to line up
     * with the base run.
     */
    @Override
    protected String dropModeSpecificColumns() {
        return " | DROP @timestamp";
    }

    // FLS drops the keyword synthetic-source delegate so a granted text field reconstructs to null

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/6714")
    public void testFieldLevelSecurityAllow() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/6714")
    public void testFieldLevelSecurityAllowPartial() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/6714")
    public void testFieldLevelSecurityPartiallyUnmappedLoad() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/6714")
    public void testFieldLevelSecurityPartiallyUnmappedNullify() throws Exception {}

    // FLS-excluded fields are still reconstructed from _ignored_source (both the except:_source and the grant-allow-list shapes)

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFieldLevelSecuritySourceDisabledMultiIndex() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFieldLevelSecuritySourceDisabledMultiIndexPartialMappingNonKeyword() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoad() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoadAndCast() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFieldLevelSecuritySourceDisabledWithUnmappedFieldsLoadAll() throws Exception {}

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/security/issues/13332")
    public void testFLS_SourceDisabled_MultiIndex_WithUnmappedFieldsLoadAll() throws Exception {}

}
