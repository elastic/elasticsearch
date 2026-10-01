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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end coverage for elastic/esql-planning#2134: a CSV dataset whose column widens partway
 * through the schema sample must say so in a response {@code Warning} header, and
 * {@code schema_resolution: strict} must refuse the widen even on a dataset whose glob resolves to a
 * single file, where cross-file reconciliation has no second file to compare against. {@code strict}
 * (like every {@code schema_resolution} value) only takes effect through the glob/multi-file resolution
 * path ({@code ExternalSourceResolver#resolveMultiFileSource}); a literal non-glob resource path
 * ({@code resolveSingleFileSource}) never consults it at all, which is why the strict case below
 * registers through a one-file glob rather than a literal path.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvWithinFileWideningIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private static Path writeBadRow3Csv(Path dir) throws Exception {
        Files.createDirectories(dir);
        Path file = dir.resolve("bad_row3.csv");
        Files.writeString(file, "a,b\n1,r1\n2,r2\noops,r3\n", StandardCharsets.UTF_8);
        return file;
    }

    /**
     * The issue's own repro, condensed: a column inferred {@code integer} from its first two rows widens
     * to {@code keyword} on a non-numeric third row, still inside the (default) schema sample. The query
     * must both return the widened value correctly AND tell the client it happened.
     */
    public void testWithinFileKeywordWideningWarnsAndSurvivesText() throws Exception {
        Path file = writeBadRow3Csv(createTempDir().resolve("within_file_widen"));
        String dataset = registerDataset("within_file_widen", StoragePath.fileUri(file), Map.of("error_mode", "null_field"));
        String query = "FROM " + dataset + " | SORT b ASC | KEEP a";

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        List<String> wideningWarnings = new CopyOnWriteArrayList<>();
        AtomicReference<List<List<Object>>> rows = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                rows.set(getValuesList(response));
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                threadContext.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .filter(w -> w.contains("column [a]") && w.contains("[keyword]") && w.contains("oops"))
                    .forEach(wideningWarnings::add);
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("query must succeed so the widening warning can reach the client", failure.get());
        }
        assertThat(rows.get().stream().map(row -> row.get(0)).toList(), equalTo(List.of("1", "2", "oops")));
        assertThat(
            "the within-file widen must reach the client via the response Warning header",
            wideningWarnings.size(),
            greaterThanOrEqualTo(1)
        );
    }

    /**
     * {@code schema_resolution: strict} refuses the same within-file widen on a dataset whose glob
     * resolves to a single file, where cross-file reconciliation has no second file to compare against
     * and so would otherwise see nothing to validate (unlike the cross-file case, which {@code strict}
     * already covered before this fix). Registered through a glob matching exactly one file, not a
     * literal path — see the class javadoc for why that distinction matters here.
     */
    public void testStrictRefusesSingleFileWithinFileWidening() throws Exception {
        Path dir = createTempDir().resolve("within_file_widen_strict");
        writeBadRow3Csv(dir);
        String dataset = registerDataset(
            "within_file_widen_strict",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "strict")
        );

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP a"))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), containsString("column [a]"));
        assertThat(ex.getMessage(), containsString("[keyword]"));
        assertThat(ex.getMessage(), containsString("[integer]"));
    }
}
