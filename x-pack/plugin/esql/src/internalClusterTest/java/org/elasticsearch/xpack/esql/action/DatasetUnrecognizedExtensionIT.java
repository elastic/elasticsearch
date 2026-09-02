/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * A dataset of objects whose extension names no registered format — the shape of a real AWS VPC Flow Logs dump,
 * which is delivered as space-delimited {@code .log.gz} with a header line.
 *
 * <p>Two behaviours are pinned here, end to end through a cluster rather than at the resolver seam:
 * <ul>
 *   <li>with an explicit {@code format}, such a dataset READS — including over a glob. Every multi-file resolve
 *       goes through the async path, which selected factories with the path-only {@code canHandle(String)} and so
 *       discarded the caller's config; {@code format} was honored for one concrete file and silently a no-op for
 *       every glob, leaving a dump like this unreadable under any configuration.</li>
 *   <li>without one, the query fails as a 400 naming the extension and the {@code format} setting — not as a 500
 *       advising that a data-source plugin be installed, which was never the cause (the scheme is validated
 *       before the factory loop, so a missing plugin cannot be what went wrong here).</li>
 * </ul>
 */
public class DatasetUnrecognizedExtensionIT extends AbstractExternalDataSourceIT {

    /** Header line plus rows, in AWS VPC Flow Logs version-2 field order, space-delimited. */
    private static final String HEADER = "version account-id interface-id srcaddr dstaddr srcport dstport protocol"
        + " packets bytes start end action log-status";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    public void testSingleConcreteFileReadsWithExplicitFormat() throws Exception {
        String glob = writeFlowLogDump().replace(
            "*.log.gz",
            "165926643534_vpcflowlogs_us-west-2_fl-0516f15db7171ba85_20220706T0000Z_06507452.log.gz"
        );

        registerDataSource("local_ds", Map.of());
        registerDataset(
            "vpcflow",
            "local_ds",
            glob,
            Map.of("format", "csv", "delimiter", " ", "header_row", "true", "schema_resolution", "first_file_wins")
        );

        try (var response = run(syncEsqlQueryRequest("FROM vpcflow | SORT bytes | KEEP srcaddr, dstaddr, bytes"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0).toString(), equalTo("10.2.105.255"));
            assertThat(((Number) rows.get(0).get(2)).longValue(), equalTo(6330L));
            assertThat(((Number) rows.get(1).get(2)).longValue(), equalTo(41269L));
        }
    }

    public void testGlobOfUnrecognizedExtensionReadsWithExplicitFormat() throws Exception {
        String glob = writeFlowLogDump();

        registerDataSource("glob_ds", Map.of());
        registerDataset("vpcflow_glob", "glob_ds", glob, Map.of("format", "csv", "delimiter", " ", "header_row", "true"));

        try (var response = run(syncEsqlQueryRequest("FROM vpcflow_glob | STATS n = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(4L));
        }
    }

    public void testGlobFirstFileWinsReadsWithExplicitFormat() throws Exception {
        String glob = writeFlowLogDump();

        registerDataSource("ffw_ds", Map.of());
        registerDataset(
            "vpcflow_ffw",
            "ffw_ds",
            glob,
            Map.of("format", "csv", "delimiter", " ", "header_row", "true", "schema_resolution", "first_file_wins")
        );

        try (var response = run(syncEsqlQueryRequest("FROM vpcflow_ffw | STATS n = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(4L));
        }
    }

    public void testGlobOfUnrecognizedExtensionWithoutFormatFailsAsBadRequest() throws Exception {
        String glob = writeFlowLogDump();

        registerDataSource("local_ds", Map.of());
        registerDataset("vpcflow_noformat", "local_ds", glob, Map.of());

        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM vpcflow_noformat | LIMIT 1"), TIMEOUT).close());

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("Cannot determine how to read"));
        // The compound tail, not the bare ".gz" — gzip IS installed here, so naming it alone would contradict itself.
        assertThat(e.getMessage(), containsString("[.log.gz]"));
        assertThat(e.getMessage(), containsString("[format]"));
        assertThat(e.getMessage(), not(containsString("plugin is installed")));
    }

    /** Two space-delimited, gzipped, header-bearing objects under one directory; returns a glob over them. */
    private String writeFlowLogDump() throws Exception {
        Path dir = createTempDir();
        writeGzipped(
            dir.resolve("165926643534_vpcflowlogs_us-west-2_fl-0516f15db7171ba85_20220706T0000Z_06507452.log.gz"),
            HEADER
                + "\n2 165926643534 eni-03bf5f4e402e08a3e 10.2.105.255 34.222.73.61 10090 51934 6 14 6330"
                + " 1657065572 1657065603 ACCEPT OK\n"
                + "2 165926643534 eni-03bf5f4e402e08a3e 10.2.105.255 10.2.69.212 56834 31016 6 52 41269"
                + " 1657065572 1657065603 ACCEPT OK\n"
        );
        writeGzipped(
            dir.resolve("165926643534_vpcflowlogs_us-west-2_fl-0516f15db7171ba85_20220706T0000Z_5f2d70fb.log.gz"),
            HEADER
                + "\n2 165926643534 eni-03bf5f4e402e08a3e 10.2.105.255 34.221.30.245 10090 44434 6 18 6569"
                + " 1657065572 1657065603 ACCEPT OK\n"
                + "2 165926643534 eni-03bf5f4e402e08a3e 10.2.105.255 10.2.74.86 59656 31016 6 7 2457"
                + " 1657065572 1657065603 ACCEPT OK\n"
        );
        return dir.toUri() + "*.log.gz";
    }
}
