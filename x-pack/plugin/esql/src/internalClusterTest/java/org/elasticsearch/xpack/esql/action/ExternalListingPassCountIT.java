/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * How many times a query asks the store to list, and how that number scales.
 * <p>
 * Listing is the dominant cost of answering a schema over a large glob, and it is invisible to a timing test on a
 * local filesystem: {@code LocalStorageProvider} walks the tree into a list before handing out the first entry, so a
 * bounded listing does exactly the work an unbounded one does and only the kept set differs. The saving a bound is
 * supposed to buy is a request never issued, which is an object-store effect. So this suite asserts the
 * <em>request count</em>, through {@link CountingStorageProvider}, and never the clock.
 * <p>
 * The shape under test is a glob whose wildcard sits <em>inside</em> a path segment — {@code date=2024-01-0*} — which
 * is what a Hive-partitioned dataset queried for a date range looks like. {@code StoragePath.patternPrefix} cuts the
 * prefix at the last {@code /} before the first metacharacter, so the store is asked to list the whole parent and
 * the matching keys are found by filtering. When the parent holds more keys than the listing bound, and they sort
 * ahead of the matches (object listings are lexicographic; date folders are oldest-first; queries ask about recent
 * data), the bounded pass matches nothing — and a bounded pass that matches nothing cannot be distinguished from an
 * empty dataset, so the expander lists again, unbounded.
 * <p>
 * That second pass is what these tests pin. One query should ask the store to list once.
 */
public class ExternalListingPassCountIT extends AbstractExternalDataSourceIT {

    /**
     * Padding folders that sort ahead of the matching ones. Above the 1,000-key {@code partition_sample_size}
     * default, so the first bounded pass is spent entirely on keys the glob cannot match.
     */
    private static final int PADDING_FOLDERS = 1_200;

    /** Folders the glob matches: {@code date=2024-01-01} .. {@code date=2024-01-09}. */
    private static final int MATCHING_FOLDERS = 9;

    /**
     * Registers {@link CountingStorageProvider} under its scheme. Storage-provider schemes are only picked up
     * for {@link DataSourcePlugin}s found via SPI, so this class is listed in the internalClusterTest
     * {@code META-INF/services} file rather than in {@link #nodePlugins()}.
     */
    public static final class CountingDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of(CountingStorageProvider.SCHEME);
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(CountingStorageProvider.SCHEME, new Factory());
        }

        private static final class Factory implements StorageProviderFactory {
            @Override
            public StorageProvider create(Settings settings) {
                return new CountingStorageProvider();
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                // No config keys are consumed, so nothing identifies this provider's configuration.
                return new Configured<>(new CountingStorageProvider(), Set.of(), "", "");
            }
        }
    }

    /**
     * Only the format reader. The base class supplies the licensed ESQL plugin, the HTTP/local data source and the
     * {@code test} validator, and {@link CountingDataSourcePlugin} arrives through {@code META-INF/services} — adding
     * any of those here registers their thread pools twice.
     */
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * One query, one listing pass.
     * <p>
     * The assertion is an exact count rather than an upper bound because the failure this guards against is a
     * second pass, and "at most one" and "exactly one" differ by exactly the bug.
     */
    public void testSchemaOnlyQueryListsOnce() throws Exception {
        String dataset = registerCountingDataset("pass_count", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat("no rows are read", getValuesList(response).size(), equalTo(0));
            // Pinned exactly: a listing that answered from the wrong files would still have the right count, and a
            // bound that silently narrowed the schema is the failure this is positioned to catch. {@code date} is
            // the Hive partition column detected from the folder names, so it is part of the answer.
            assertThat(columnsOf(response), equalTo(List.of("id:integer", "v:keyword", "date:keyword")));
        }

        assertThat(
            "a query that only needs a schema must ask the store to list once; a second pass means a bounded "
                + "listing matched nothing and was thrown away",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * The same query over a dataset with ten times the padding must still list once. The count is a property of the
     * query, not of how much unrelated data shares the prefix — which is precisely what regressed.
     */
    public void testListingPassesDoNotGrowWithTheDataset() throws Exception {
        String small = registerCountingDataset("pass_small", 0);
        String large = registerCountingDataset("pass_large", PADDING_FOLDERS * 10);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + small + " | LIMIT 0").close();
        long smallPasses = CountingStorageProvider.listingPasses();

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + large + " | LIMIT 0").close();
        long largePasses = CountingStorageProvider.listingPasses();

        // Both asserted absolutely rather than against each other. Equality alone can hold because both regressed
        // together, which would report a pass for the wrong reason; these say what each number must be.
        assertThat("a glob with no unrelated keys ahead of its matches lists once", smallPasses, equalTo(1L));
        assertThat("listing passes must not scale with how many unrelated keys share the prefix", largePasses, equalTo(1L));
    }

    /**
     * A row-reading query over the same glob. Resolution and split discovery each hold a listing extent, so this
     * covers the second of the two listings the subsystem performs.
     */
    public void testRowReadingQueryDoesNotListRepeatedly() throws Exception {
        String dataset = registerCountingDataset("pass_rows", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 3")) {
            assertThat(getValuesList(response).size(), equalTo(3));
        }

        assertThat(
            "resolution and split discovery must not each pay a discarded bounded pass",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * A second query over the same dataset must ask the store nothing.
     * <p>
     * This is the half of the cost a bound used to forfeit. A bounded listing took the uncached rail outright, so
     * however complete it was it was recomputed on every execution -- on the ticket's own dataset that is 4082 date
     * folders re-listed to answer a nine-file schema, every time, which is why the regression never felt like a cold
     * cost. A bounded listing that drained its whole prefix is complete, and a complete listing is the same artifact
     * whoever asked for it, so it belongs in the shared cache.
     */
    public void testASecondQueryListsNothing() throws Exception {
        String dataset = registerCountingDataset("pass_warm", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat(columnsOf(response), equalTo(List.of("id:integer", "v:keyword", "date:keyword")));
        }

        // Exactly zero, not "fewer": the listing is either served from the cache or it is not, and the whole point
        // is that a complete bounded listing is cacheable. One pass here means it was thrown away again.
        assertThat(
            "a repeat query must be served from the listing cache and ask the store nothing",
            CountingStorageProvider.listingPasses(),
            equalTo(0L)
        );
        assertThat("and visit no keys", CountingStorageProvider.keysVisited(), equalTo(0L));
    }

    /**
     * A truncated listing must still never be cached. The bound here is below the number of matching files, so the
     * drain stops on matches it has rather than on exhaustion and the listing is a prefix of the dataset. Served to
     * a later query it would pass for the whole of it, with nothing in it to say otherwise -- so the second query
     * must list again.
     */
    public void testATruncatedListingIsNotCached() throws Exception {
        String dataset = registerCountingDataset("pass_truncated", 0, Map.of("partition_sample_size", 2));
        String coordinator = internalCluster().getNodeNames()[0];

        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();

        assertThat(
            "a truncated listing is a prefix of the dataset and must not be served to a later query",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * Runs {@code query} on a named node. The listing cache is a per-node singleton and the resolver runs on
     * whichever node coordinates, so a test that counts listings has to pin the coordinator or it counts a
     * different node's work from one run to the next.
     */
    private EsqlQueryResponse query(String coordinator, String query) {
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet();
    }

    private static List<String> columnsOf(EsqlQueryResponse response) {
        return response.columns().stream().map(c -> c.name() + ":" + c.type().esType()).toList();
    }

    /**
     * Writes {@code paddingFolders} folders that sort ahead of the matched ones, then the nine the glob matches.
     * The padding is {@code date=2023-*}: object listings are lexicographic, so it all sorts in front of the
     * {@code date=2024-01-0*} the glob selects.
     */
    private Path writeFixture(int paddingFolders) throws IOException {
        Path dir = createTempDir();
        for (int f = 0; f < paddingFolders; f++) {
            Path folder = dir.resolve(String.format(Locale.ROOT, "date=2023-%06d", f));
            Files.createDirectories(folder);
            Files.writeString(folder.resolve("part.csv"), "id,v\n" + f + ",pad\n");
        }
        for (int d = 1; d <= MATCHING_FOLDERS; d++) {
            Path folder = dir.resolve(String.format(Locale.ROOT, "date=2024-01-%02d", d));
            Files.createDirectories(folder);
            Files.writeString(folder.resolve("part.csv"), "id,v\n" + d + ",hit\n");
        }
        return dir;
    }

    /** Registers the fixture's glob under {@code first_file_wins}, so the listing is eligible for the bound. */
    private String registerCountingDataset(String name, int paddingFolders) throws IOException {
        return registerCountingDataset(name, paddingFolders, Map.of());
    }

    private String registerCountingDataset(String name, int paddingFolders, Map<String, Object> extraSettings) throws IOException {
        Path dir = writeFixture(paddingFolders);
        String glob = CountingStorageProvider.SCHEME + "://" + dir.toAbsolutePath() + "/date=2024-01-0*/*.csv";
        Map<String, Object> settings = new HashMap<>(Map.of("format", "csv", "schema_resolution", "first_file_wins"));
        settings.putAll(extraSettings);
        return registerDataset(name, glob, settings);
    }
}
