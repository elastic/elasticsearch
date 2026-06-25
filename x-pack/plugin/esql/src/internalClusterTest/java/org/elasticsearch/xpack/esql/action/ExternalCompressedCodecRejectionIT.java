/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * End-to-end guard for the GA text-format codec gate: on release builds an
 * {@code EXTERNAL "...csv.bz2"} query must be rejected at planning time with the
 * {@code "compression codec [bzip2] is not supported; supported: uncompressed, gzip, zstd"} message produced by
 * {@code FormatReaderRegistry.byExtension}. bzip2 stands in for the four codecs the gate removes from the GA
 * surface (bzip2/snappy/lz4/brotli); it is the only one wired up as a node plugin in this module.
 *
 * <p>Unlike the snapshot-only QA REST suites, this {@code internalClusterTest} task force-enables the
 * {@code esql_external_datasources} feature flag (see {@code build.gradle}) on both snapshot and release builds, so
 * it is the only home where the release-only rejection actually executes through the full planner path today. The
 * rejection fires during schema resolution ({@code FileSourceFactory.resolveMetadata} resolves the format reader
 * before opening the object), so the placeholder {@code .csv.bz2} file is never read.
 *
 * <p>Release-only by construction: on snapshot builds the gate is bypassed and bzip2 is allowed, so the assertion
 * is skipped via {@link org.junit.Assume}. The snapshot-side positive coverage lives in
 * {@code ExternalFileBzip2NdJsonCountIT}; the matching release/snapshot unit coverage of the same code path lives in
 * {@code DataSourceModuleTests}.
 */
public class ExternalCompressedCodecRejectionIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /**
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} overrides {@link ExtensiblePlugin#loadExtensions} with a no-op;
     * re-delegating to {@code super} lets {@code MockPluginsService} aggregate the data-source SPI implementations.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        // Bzip2 must be registered so the codec resolves and the build-type gate (not a "no such codec" error) fires.
        plugins.add(Bzip2DataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testBzip2CsvRejectedOnReleaseBuild() throws IOException {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeFalse("snapshot builds allow the full codec set; the gate is release-only", Build.current().isSnapshot());

        // The gate fires before the object is opened, so the file content is irrelevant; the bytes are never read.
        Path file = createTempDir().resolve("rejected.csv.bz2");
        Files.write(file, "placeholder".getBytes(StandardCharsets.UTF_8));

        String query = "EXTERNAL \"" + StoragePath.fileUri(file) + "\" | LIMIT 1";
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query), TIMEOUT).close());
        assertCauseMessageContains(ex, "compression codec [bzip2] is not supported; supported: uncompressed, gzip, zstd");
    }

    /** Walks the cause chain and asserts a message fragment appears somewhere in it. */
    private static void assertCauseMessageContains(Throwable throwable, String fragment) {
        Throwable cause = throwable;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains(fragment) == false)) {
            cause = cause.getCause();
        }
        assertThat("error chain should contain message fragment [" + fragment + "]", cause, org.hamcrest.Matchers.notNullValue());
    }
}
