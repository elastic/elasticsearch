/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * What a short row tells the client, end to end: a dropped row and a padded row each have to reach a response
 * {@code Warning} header, since the row itself says nothing about what happened to it -- a padded null is
 * indistinguishable from a null the file carries, and a dropped row is simply absent.
 *
 * <p>The csv-spec cases over the same fixture look like they pin this and cannot: {@code warningRegex:} selects
 * {@code AssertWarnings.AllowedRegexes}, which walks the warnings it RECEIVED and checks each against the
 * patterns, so a response carrying no warning at all passes every pattern vacuously. These assertions fail when
 * the notice is not emitted, which is the half that was missing -- the reader unit tests pin the strings, and
 * this pins their delivery. It is the same shape as {@code ExternalCsvWithinFileWideningIT}, which pins the
 * widening warning the same way.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvShortRowWarningsIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Row 2 carries two of the header's three fields; rows 1 and 3 are complete. */
    private static Path writeRaggedCsv(Path dir, String name) throws Exception {
        Files.createDirectories(dir);
        Path file = dir.resolve(name);
        Files.writeString(file, "id,name,score\n1,alice,10\n2,bob\n3,carol,30\n", StandardCharsets.UTF_8);
        return file;
    }

    /** Under {@code skip_row} the short row is dropped, and the client is told which row and both widths. */
    public void testDroppedShortRowIsReportedToTheClient() throws Exception {
        Path file = writeRaggedCsv(createTempDir().resolve("ragged_skip"), "ragged.csv");
        String dataset = registerDataset(
            "ragged_short_skip",
            StoragePath.fileUri(file),
            Map.of("error_mode", "skip_row", "max_errors", 10)
        );

        List<String> warnings = warningsOf("FROM " + dataset + " | KEEP id, name | SORT id", 2);
        assertThat(warnings, hasItem(containsString("missing trailing fields: [2] columns, the header has [3]")));
        assertThat(warnings, hasItem(containsString("cannot be read; skipping them")));
    }

    /**
     * Under {@code pad_short_rows} the row is kept with its missing field null, and that is reported on its own
     * channel -- so it is reported here under the default {@code fail_fast}, where the error policy's channel is
     * a no-op. Nothing is padded silently.
     */
    public void testPaddedShortRowIsReportedToTheClient() throws Exception {
        Path file = writeRaggedCsv(createTempDir().resolve("ragged_pad"), "ragged.csv");
        String dataset = registerDataset("ragged_short_padded", StoragePath.fileUri(file), Map.of("pad_short_rows", true));

        List<String> warnings = warningsOf("FROM " + dataset + " | KEEP id, name | SORT id", 3);
        assertThat(warnings, hasItem(containsString("missing trailing fields, read as null: [2] columns, the header has [3]")));
        assertThat(warnings, hasItem(containsString("reading those fields as null")));
    }

    /**
     * Runs {@code query} on one node and returns the {@code Warning} headers that node's response carried,
     * asserting the row count first so a query that returned nothing cannot pass for one that warned.
     */
    private List<String> warningsOf(String query, int expectedRows) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Integer> rows = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                rows.set(getValuesList(response).size());
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("the query must succeed so its warnings can reach the client", failure.get());
        }
        assertThat("rows returned", rows.get(), equalTo(expectedRows));
        return warnings;
    }
}
