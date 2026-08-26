/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-format guardrail for the two properties of the read-shape identity that are NOT about blocking contamination:
 * that it does not split the cache when it should not, and that a multiset listing folds per position.
 *
 * <p>Anti-fragmentation matters as much as the correctness half and is easier to break silently: an identity that
 * splits too eagerly costs a cold scan on every mapped dataset and no correctness test would notice.
 *
 * <p>On what each test is worth. {@link #testDuplicateListingColdEqualsWarm} is mutation-proven — revert the
 * aggregate to folding one copy per unique path and it goes red. The two warmth tests are NOT: two attempts to
 * fragment the fingerprint failed to redden them, because the values perturbed do not differ between an inferred
 * read and a no-op declaration in the first place. They still assert a real and desirable property end to end — that
 * declaring nothing new keeps the warm entry — and the encoder half of it is pinned directly by
 * {@code ReadShapeFingerprintTests#testNoOpRedeclarationSharesTheInferredShape}. Read them as behavioural
 * documentation with a live assertion, not as proof the identity cannot fragment.
 *
 * <p>Contamination is pinned separately, in {@link ExternalReadShapeContaminationIT}. It does not live here because
 * the constructions that look like they would prove it — two datasets both counting rows, or a per-column aggregate
 * over a retyped column — cannot fail: {@code COUNT(*)} parses no column, and a retyped column is already
 * safe-missed by the declared-overlay poison and its harvest stripped as a union_by_name pin.
 */
// numDataNodes=1: the warm cache is a per-node singleton, so a single node is what pins the coordinator. A random
// coordinator lets the second query land on an empty cache and re-scan, which passes for the wrong reason.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractExternalReadShapeParityIT extends AbstractExternalDataSourceIT {

    protected static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);
    protected static final int ROWS = 200;

    private static final String SRC = "read_shape_src";
    private final List<String> datasets = new ArrayList<>();

    /** The dataset {@code format} setting, e.g. {@code "csv"}. */
    protected abstract String format();

    /** Fixture file extension, e.g. {@code ".csv"}. */
    protected abstract String fileExtension();

    /** Fixture text: {@code id} running {@code 0..rows-1} and a text {@code age} column. */
    protected abstract String buildContent(int rows);

    @Before
    public void registerReadShapeDataSource() {
        registerDataSource(SRC, Map.of());
    }

    @After
    public void cleanupReadShapeDatasets() {
        for (String dataset : datasets) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset }))
                    .actionGet();
            } catch (Exception ignored) {
                // best-effort teardown, mirroring the base cleanupRegistry()
            }
        }
    }

    /**
     * Declaring a column as exactly what inference already produced describes the SAME read, so the two datasets must
     * keep sharing one warm entry. If this splits, every mapped dataset pays a cold scan for declaring nothing.
     */
    public void testNoOpRedeclarationStillSharesTheWarmEntry() throws Exception {
        DatasetMapping sameAsInferred = mappingOf("age", new DatasetFieldMapping("keyword", null));
        String file = writeFixture("noop");
        String inferred = register("noop_inferred", file, null);
        String declared = register("noop_declared", file, sameAsInferred);

        assertCount(inferred, ROWS, null);
        assertCount(declared, ROWS, 0L);
    }

    /**
     * A pure rename moves no value a statistic measures — harvested statistics are keyed by the file's own column
     * names on every rail — so a renamed dataset must keep the warmth of its unrenamed twin.
     */
    public void testRenameOnlyKeepsTheWarmEntry() throws Exception {
        DatasetMapping renameOnly = mappingOf("years", new DatasetFieldMapping("keyword", "age"));
        String file = writeFixture("rename");
        String plain = register("rename_plain", file, null);
        String renamed = register("rename_renamed", file, renameOnly);

        assertCount(plain, ROWS, null);
        assertCount(renamed, ROWS, 0L);
    }

    /**
     * A comma list naming one file twice reads it twice, so its count is the multiset total. Cold and warm must
     * agree: folding one copy per unique path returned a different number for the same query depending only on cache
     * state.
     */
    public void testDuplicateListingColdEqualsWarm() throws Exception {
        Path file = createTempDir().resolve("dupes" + fileExtension());
        Files.writeString(file, buildContent(ROWS));
        String uri = StoragePath.fileUri(file);
        String dataset = register("dupes", uri + "," + uri, null);

        long cold = count(dataset);
        assertThat("a file listed twice is read twice", cold, equalTo(2L * ROWS));
        assertThat("warm must agree with cold — the same query cannot depend on cache state", count(dataset), equalTo(cold));
    }

    private long count(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true), TIMEOUT)) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    /** Asserts {@code COUNT(*)}, and when {@code expectedScanned} is non-null, that the read scanned exactly that much. */
    private void assertCount(String dataset, long expectedCount, Long expectedScanned) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true), TIMEOUT)) {
            assertThat(
                "COUNT(*) for [" + dataset + "]",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo(expectedCount)
            );
            if (expectedScanned != null) {
                assertThat("scan rows for [" + dataset + "] (0 == served warm)", response.documentsFound(), equalTo(expectedScanned));
            }
        }
    }

    private String writeFixture(String name) throws Exception {
        Path file = createTempDir().resolve(name + fileExtension());
        Files.writeString(file, buildContent(ROWS));
        return StoragePath.fileUri(file);
    }

    private String register(String name, String uri, DatasetMapping mapping) {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("format", format());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, SRC, uri, null, settings, mapping)
            )
        );
        datasets.add(name);
        return name;
    }

    private static DatasetMapping mappingOf(String column, DatasetFieldMapping fieldMapping) {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put(column, fieldMapping);
        return new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
    }
}
