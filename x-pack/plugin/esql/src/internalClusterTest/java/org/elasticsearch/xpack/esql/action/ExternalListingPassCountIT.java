/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * How many times a query asks the store to list, and how that number scales.
 * <p>
 * Listing is the dominant cost of answering a schema over a large glob. This suite asserts the <em>request count</em>
 * through {@link CountingStorageProvider}, which carries why a count and not a duration is the only thing worth
 * asserting here.
 * <p>
 * The shape under test is a glob whose wildcard sits <em>inside</em> a path segment — {@code date=2024-01-0*} — which
 * is what a Hive-partitioned dataset queried for a date range looks like. {@code StoragePath.patternPrefix} cuts the
 * prefix at the last {@code /} before the first metacharacter, so the prefix the store is asked about is wider than
 * the set the pattern matches.
 * <p>
 * The defect these tests exist for came from deciding things about the narrow set from the wide one. The bound used
 * to count keys the store returned: the parent held more keys than the bound and they sorted ahead of the matches
 * (object listings are lexicographic; date folders are oldest-first; queries ask about recent data), so the bounded
 * pass matched nothing — and a bounded pass that matches nothing cannot be told apart from an empty dataset, so the
 * expander listed again, unbounded, on every single execution. The bound now counts matching files.
 * <p>
 * So one query should ask the store to list once, a repeat should ask nothing, and neither number should move when
 * unrelated keys are added under the same prefix.
 */
public class ExternalListingPassCountIT extends AbstractExternalDataSourceIT {

    /**
     * Padding folders that sort ahead of the matching ones, and more of them than the 1,000 of the
     * {@code partition_sample_size} default — which is what made the old key-counting bound stop before reaching a
     * match. Kept at that size because it is the fixture the defect needs: a bound that counts matches is not spent
     * on them, and a provider that narrows on the pattern's literal lead is never handed them at all.
     */
    private static final int PADDING_FOLDERS = 1_200;

    /** Folders the glob matches: {@code date=2024-01-01} .. {@code date=2024-01-09}. */
    private static final int MATCHING_FOLDERS = 9;

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
        long smallKeys = CountingStorageProvider.keysVisited();

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + large + " | LIMIT 0").close();
        long largePasses = CountingStorageProvider.listingPasses();
        long largeKeys = CountingStorageProvider.keysVisited();

        // Both asserted absolutely rather than against each other. Equality alone can hold because both regressed
        // together, which would report a pass for the wrong reason; these say what each number must be.
        assertThat("a glob with no unrelated keys ahead of its matches lists once", smallPasses, equalTo(1L));
        assertThat("listing passes must not scale with how many unrelated keys share the prefix", largePasses, equalTo(1L));

        // Keys as well as passes, because a pass is not what an object store bills. This double has no pages, so one
        // listObjects over a prefix of any width is one pass here and one request per thousand keys there -- a count
        // of passes cannot see a drain that walked the whole prefix. These are upper bounds rather than equalities:
        // what must hold is that the work follows the matching set, and the fixture's padding is what would move it.
        assertThat("a small glob visits about its own matches", smallKeys, lessThanOrEqualTo((long) MATCHING_FOLDERS * 2));
        assertThat(
            "and neither does the key count scale with unrelated folders sharing the prefix",
            largeKeys,
            lessThanOrEqualTo((long) MATCHING_FOLDERS * 2)
        );
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
     * A second bounded query over the same dataset must ask the store nothing.
     * <p>
     * This is the half of the cost a bound used to forfeit: a bounded listing took the uncached rail outright, so
     * however complete it was it was recomputed on every execution. This dataset's glob carries no partition-value
     * hints, so the partition walk never runs on either rail and a bounded listing that drained its whole prefix is
     * the same artifact as an unbounded one of the same inputs — it belongs in the shared cache and may be served
     * back. Where the walk is in play that equivalence does not hold and the listing is kept out; see
     * {@code DatasetListingServiceCacheTests}.
     */
    public void testASecondQueryListsNothing() throws Exception {
        String dataset = registerCountingDataset("pass_warm", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();
        // This test's own positive control for the two counters it can have one for: both are observed non-zero
        // on the same dataset and the same code path before being asserted zero below, so a counter that stopped
        // recording, or a fixture that stopped reaching the store, cannot read as a cache hit.
        assertThat("the cold query listed", CountingStorageProvider.listingPasses(), greaterThan(0L));
        assertThat("and visited keys", CountingStorageProvider.keysVisited(), greaterThan(0L));
        // Not childListings: this query is bounded, and only an unbounded listing fans out or walks - the glob
        // carrying no partition-value hints is what rules out the walk, not the fan-out, which
        // testAnAggregateDoesNotListOncePerPartitionFolder drives to a non-zero count on this same glob. So there
        // is no non-zero reading of it to be had here, and asserting it zero below is a guard against either of
        // them appearing on a repeat query rather than a cache measurement.

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat(columnsOf(response), equalTo(List.of("id:integer", "v:keyword", "date:keyword")));
        }

        assertThat(
            "a repeat query must be served from the listing cache and ask the store nothing",
            CountingStorageProvider.listingPasses(),
            equalTo(0L)
        );
        assertThat("and visit no keys", CountingStorageProvider.keysVisited(), equalTo(0L));
        // Both kinds of request, because "nothing" is the claim: the fan-out and the partition walk ask for
        // children rather than objects, so a listing-pass count alone would let either of them back in unseen.
        assertThat("and ask for no directory children", CountingStorageProvider.childListings(), equalTo(0L));
    }

    /**
     * The positive control for the half that reads: an unbounded query is served too. Without this, the assertion
     * above passes equally well if the cache stopped working for every shape rather than one.
     */
    public void testAnUnboundedRepeatQueryListsNothing() throws Exception {
        String dataset = registerCountingDataset("pass_warm_unbounded", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        query(coordinator, "FROM " + dataset + " | STATS c = COUNT(*)").close();

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | STATS c = COUNT(*)")) {
            assertThat(getValuesList(response).get(0).get(0), equalTo((long) MATCHING_FOLDERS));
        }

        assertThat("a repeat aggregate is served from the listing cache", CountingStorageProvider.listingPasses(), equalTo(0L));
    }

    /**
     * An entry the cache holds is served to a repeat of the query that produced it, and not to a query whose own
     * listing would have stopped on its bound.
     * <p>
     * Both halves are about one entry and are asserted together so a failure says which direction broke. The glob
     * is {@code date=*} rather than the suite's usual {@code date=2024-01-0*} because a partition-value filter
     * narrows a listing only under a fully-wildcard segment, so this is the shape where the entry the cache holds
     * is narrower than the pattern it was listed from — here one folder of 1,209.
     * <p>
     * An equality on a fully-wildcard segment is spliced into the glob itself, so this query lists
     * {@code date=2024-01-05/*.csv} and its entry holds that folder's one file. The rewritten pattern is a field of
     * the listing's address, so the repeat finds the entry and the unfiltered query cannot: it has a different
     * address and must list for itself. That is the failure the second assertion exists for — served the filtered
     * entry, it would infer a schema, and a partition column's type, from one folder of 1,209 — and it is the
     * address that stops it, which is also what the test below turns on. The entry's width is no defence either
     * way: one file is far below this query's bound.
     */
    public void testAFilteredEntryServesTheSameFilterAndNoOther() throws Exception {
        Path dir = writeFixture(PADDING_FOLDERS);
        String glob = CountingStorageProvider.location(dir) + "/date=*/*.csv";
        String dataset = registerDataset("pass_filtered", glob, Map.of("format", "csv", "schema_resolution", "first_file_wins"));
        String coordinator = internalCluster().getNodeNames()[0];
        String filtered = "FROM " + dataset + " | WHERE date == \"2024-01-05\" | LIMIT 0";

        query(coordinator, filtered).close();

        CountingStorageProvider.reset();
        query(coordinator, filtered).close();
        assertThat("a repeat of the query that produced the entry must be served it", CountingStorageProvider.listingPasses(), equalTo(0L));

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();
        assertThat(
            "a query without that filter must not be served a listing narrowed by it",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * A complete listing wider than this query's bound is not served to it.
     * <p>
     * The aggregate lists unbounded, so the entry is the whole pattern: nine matching files. The schema query's
     * bound is three, so its own listing would have stopped on three of them and inferred the schema, and the
     * partition column's type, from those — while the entry reports all nine. Serving it would make the answer
     * depend on whether an aggregate ran first, which is the shape of the bug that made the bounded rail and the
     * cache mutually exclusive in the first place. Paired with {@code testASecondQueryListsNothing}: either test
     * alone passes against a rule that always serves or always declines.
     */
    public void testAnEntryWiderThanTheBoundIsNotServed() throws Exception {
        String dataset = registerCountingDataset("pass_wider", 0, Map.of("partition_sample_size", 3));
        String coordinator = internalCluster().getNodeNames()[0];

        query(coordinator, "FROM " + dataset + " | STATS c = COUNT(*)").close();

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat(columnsOf(response), equalTo(List.of("id:integer", "v:keyword", "date:keyword")));
        }

        assertThat(
            "an entry listing nine matches cannot answer a query whose drain stops at three",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * A glob that matches nothing must still list once; a mistyped glob used to cost two full listings.
     * <p>
     * One pass suffices because a drain that exhausted its prefix saw every key under it, so an empty result is the
     * dataset's answer. Here the store narrows to the pattern's literal lead and returns nothing, which exhausts
     * the prefix under a keys-visited bound too — so this stays green with that bound restored. What discriminates
     * is {@code GlobExpanderTests.testABoundCountsMatchesSoAFullDrainIsNotTruncated}, whose stub cannot narrow.
     */
    public void testAGlobThatMatchesNothingListsOnce() throws Exception {
        Path dir = writeFixture(PADDING_FOLDERS);
        String glob = CountingStorageProvider.location(dir) + "/date=2025-01-0*/*.csv";
        String dataset = registerDataset("pass_empty", glob, Map.of("format", "csv", "schema_resolution", "first_file_wins"));
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        // The outcome is not what this pins — an empty dataset may answer or may fail, and either is someone
        // else's test. What must hold is that deciding it took one pass.
        try (EsqlQueryResponse response = query(coordinator, "FROM " + dataset + " | LIMIT 0")) {
            assertThat(getValuesList(response).size(), equalTo(0));
        } catch (Exception e) {
            // An empty glob that resolves to no schema is a legitimate failure; the pass count below still applies.
        }

        assertThat(
            "an empty bounded pass that drained its prefix is conclusive and must not be re-listed",
            CountingStorageProvider.listingPasses(),
            equalTo(1L)
        );
    }

    /**
     * The declared-schema rail takes a bound too, and the same once-only rule holds there.
     * <p>
     * A declaration is the whole schema, so this listing reads no file for it — it counts files and derives
     * partition columns. {@code schema_resolution} has to be set explicitly: {@code DatasetRewriter} hydrates
     * {@code union_by_name} onto a dataset that omits it, and {@code union_by_name} spans every file and so never
     * takes a bound, which would make this test pass while exercising the unbounded rail.
     */
    public void testADeclaredSchemaQueryListsOnce() throws Exception {
        Path dir = writeFixture(PADDING_FOLDERS);
        String glob = CountingStorageProvider.location(dir) + "/date=2024-01-0*/*.csv";
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("v", new DatasetFieldMapping("keyword", null));
        String dataset = registerStrictDataset(
            "pass_declared",
            glob,
            properties,
            Map.of("format", "csv", "schema_resolution", "first_file_wins")
        );
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + dataset + " | LIMIT 0").close();

        assertThat("the declared rail must not pay a discarded bounded pass either", CountingStorageProvider.listingPasses(), equalTo(1L));
    }

    /**
     * A dataset-wide aggregate must not pay a request per partition folder it cannot match.
     * <p>
     * {@code COUNT(*)} resolves as {@code EAGER_STATS}, and {@code listingExtentsFor} refuses a bound for that
     * demand outright: the fold is over every file, so no prefix answers it. That makes an aggregate the one shape
     * neither the bound nor the listing cache can rescue on a cold node, and the prefix fan-out is reached only by
     * an unbounded listing — so this is the only query shape that exercises it, and nothing else here would notice
     * it issuing one {@code listObjects} per child of the pattern prefix.
     * <p>
     * The counts are pinned to the matching geometry rather than to each other. Equality alone would hold if both
     * grew to one request per folder, which is exactly the regression.
     */
    public void testAnAggregateDoesNotListOncePerPartitionFolder() throws Exception {
        String small = registerCountingDataset("agg_small", 0);
        String large = registerCountingDataset("agg_large", PADDING_FOLDERS);
        String coordinator = internalCluster().getNodeNames()[0];

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + small + " | STATS c = COUNT(*)")) {
            assertThat(getValuesList(response).get(0).get(0), equalTo((long) MATCHING_FOLDERS));
        }
        long smallPasses = CountingStorageProvider.listingPasses();

        CountingStorageProvider.reset();
        try (EsqlQueryResponse response = query(coordinator, "FROM " + large + " | STATS c = COUNT(*)")) {
            assertThat(
                "the padding holds no matching file, so the answer is the same",
                getValuesList(response).get(0).get(0),
                equalTo((long) MATCHING_FOLDERS)
            );
        }
        long largePasses = CountingStorageProvider.listingPasses();

        long largeChildListings = CountingStorageProvider.childListings();

        CountingStorageProvider.reset();
        query(coordinator, "FROM " + large + " | STATS c = COUNT(*)").close();
        long largeWarm = CountingStorageProvider.listingPasses();
        long largeWarmChildListings = CountingStorageProvider.childListings();

        assertThat("the fan-out drains one matching folder per pass", smallPasses, equalTo((long) MATCHING_FOLDERS));
        assertThat(
            "a folder the glob cannot match must not cost a request: passes follow the matching set, not the prefix",
            largePasses,
            equalTo((long) MATCHING_FOLDERS)
        );
        assertThat("and a repeat aggregate is served from the listing cache", largeWarm, equalTo(0L));
        // The positive control for every "asks the store nothing" assertion in this suite, including the
        // delimiter-listing zero in testASecondQueryListsNothing: the fan-out asks for a prefix's children, so
        // this counter demonstrably moves. Without it, a zero proves only that nothing was ever counted.
        assertThat("the fan-out asks for the prefix's children", largeChildListings, greaterThan(0L));
        assertThat("and a repeat asks for none", largeWarmChildListings, equalTo(0L));
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
        String glob = CountingStorageProvider.location(dir) + "/date=2024-01-0*/*.csv";
        Map<String, Object> settings = new HashMap<>(Map.of("format", "csv", "schema_resolution", "first_file_wins"));
        settings.putAll(extraSettings);
        return registerDataset(name, glob, settings);
    }
}
