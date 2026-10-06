/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.ExternalPlanningReservation;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.FileSplitProvider;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.PartitionValueLayout;
import org.elasticsearch.xpack.esql.datasources.Phase2Reservation;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase-2 planning reservations. Local queries enter through {@link ComputeService#startPhase2OrSkip}, the
 * same call {@code execute} makes, which charges resolved {@link ExternalSourceExec} file lists before
 * discovery. Fragment discovery charges the relation file count. A warm skip charges nothing. Phase-2 bytes
 * belong to one {@link ExternalPlanningReservation.Run}, and so does the listing split discovery performs for
 * itself; resolution's own listing stays until the reservation closes.
 */
public class ExternalPlanningBreakerTests extends ESTestCase {

    /**
     * The pre-charge counts the files the plan carries, which is only the dataset when the schema's listing was
     * complete. Over a prefix it stands down: discovery replaces that list with the set it discovers for itself
     * before anything is built over it, so the prefix's survivor maps and split shells are never allocated and the
     * provider charges the discovered count instead. Charging here as well would reserve for structures that do
     * not exist, and a run that trips on them would refuse a query the node could have served.
     */
    public void testLocalPhase2PreChargeStandsDownForAPrefix() throws Exception {
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, new AtomicInteger());
        EsqlExecutionInfo info = executionInfo();
        ExternalSourceExec exec = relation(truncatedFiles(2), Map.of()).toPhysicalExec();
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(exec, configuration(), info, () -> false, run, done);

        done.actionGet(30, TimeUnit.SECONDS);
        assertEquals("a prefix's phase-2 structures are never built, so nothing is reserved for them", 0L, run.held());
        assertEquals(baseline, breaker.getUsed());
    }

    /**
     * Charge for one listing at the default discovered-files cap, on the compacted form planning keeps.
     * Listing charge is {@link ExternalSourceResolver#listingPlanningCharge} (320 bytes of schema-map
     * slack per file). Phase 2 is one {@link Phase2Reservation#SHELL_BYTES} shell per file.
     * Together with the compacted listing that stays a few megabytes of slack under a 1 GB request breaker.
     */
    public void testPlanningChargeAtDefaultDiscoveredFilesCap() {
        int files = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(Settings.EMPTY);
        assertEquals(25_000, files);
        List<StorageEntry> entries = new ArrayList<>(files);
        for (int i = 0; i < files; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/part-" + i + ".parquet"), 1024L, Instant.EPOCH));
        }
        FileList raw = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");
        FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
        assertEquals(files, compact.fileCount());

        long listing = ExternalSourceResolver.listingPlanningCharge(compact);
        long phase2 = files * Phase2Reservation.SHELL_BYTES;
        long oneGbRequestBreaker = ByteSizeValue.ofGb(1).getBytes() * 60 / 100;
        assertThat(listing + phase2, lessThan(oneGbRequestBreaker));
        logger.info(
            "25k-file planning charge: listing=[{}] phase2=[{}] total=[{}] oneGbRequestBreaker=[{}]",
            listing,
            phase2,
            listing + phase2,
            oneGbRequestBreaker
        );
    }

    public void testLocalPhase2ChargesResolvedFilesBeforeDiscovery() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        ExternalSourceExec exec = relation(resolvedFiles(2), Map.of()).toPhysicalExec();
        assertFalse(ComputeService.canSkipSplitDiscovery(exec, service.formatReaderRegistry()));
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(exec, configuration(), info, () -> false, run, done);

        done.actionGet(30, TimeUnit.SECONDS);
        long phase2 = Phase2Reservation.bytesFor(exec.output(), exec.fileList());
        assertEquals(2 * Phase2Reservation.SHELL_BYTES, phase2);
        assertEquals(1, discoveries.get());
        assertEquals(baseline + phase2, breaker.getUsed());
        assertEquals(phase2, run.held());
    }

    public void testLocalPhase2TripsBeforeDiscoveryAndLeavesLedgerAtZero() {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1b");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        ExternalSourceExec exec = relation(resolvedFiles(2), Map.of()).toPhysicalExec();
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(exec, configuration(), info, () -> false, run, done);
        CircuitBreakingException broke = expectThrows(CircuitBreakingException.class, () -> done.actionGet(30, TimeUnit.SECONDS));

        assertThat(broke.getMessage(), containsString(EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL));
        assertEquals(0, discoveries.get());
        assertEquals(0L, run.held());
        assertEquals(baseline, breaker.getUsed());
    }

    /**
     * A fragment's relation carries what discovery settled on, not the listing it was handed. Handed a one-file prefix
     * of the schema's, discovery here lists two files for itself and plans none of them, and does not certify that as
     * a prune - so the fall-through reads the relation's listing whole. Which listing that is decides what gets read:
     * the prefix would read one file where discovery found two, and a matching row in the other would be missing from
     * the answer with nothing to say so. The top-level path already reads the discovered set; this pins the fragment
     * path to the same.
     */
    public void testAFragmentFallThroughReadsWhatDiscoveryFoundNotThePrefix() throws Exception {
        FileList prefix = truncatedFiles(1);
        FileList discovered = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("file:///f0.parquet"), 1000L, Instant.EPOCH),
                new StorageEntry(StoragePath.of("file:///f1.parquet"), 1000L, Instant.EPOCH)
            ),
            "file:///*.parquet"
        );
        ComputeService service = service(
            requestBreaker("1gb"),
            ctx -> new SplitDiscoveryResult(List.of(), 0, false, 0L, discovered, Map.of(), List.of())
        );
        EsqlExecutionInfo info = executionInfo();
        FragmentExec fragment = new FragmentExec(relation(prefix, Map.of()));
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, bind(info, requestBreaker("1gb")).openRun(), done);

        PhysicalPlan settled = done.actionGet(30, TimeUnit.SECONDS).plan();
        List<FileList> carried = new ArrayList<>();
        settled.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(ExternalRelation.class, r -> carried.add(r.fileList())));
        assertEquals(1, carried.size());
        assertSame("the fall-through reads the set discovery found, not the schema's prefix", discovered, carried.get(0));
    }

    public void testFragmentWorkChargesRelationFileCount() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        ExternalRelation external = relation(resolvedFiles(3), Map.of());
        FragmentExec fragment = new FragmentExec(external);
        assertFalse(ComputeService.canSkipSplitDiscovery(fragment, service.formatReaderRegistry()));
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, run, done);

        done.actionGet(30, TimeUnit.SECONDS);
        long phase2 = Phase2Reservation.bytesFor(external.output(), external.fileList());
        assertEquals(3 * Phase2Reservation.SHELL_BYTES, phase2);
        assertEquals(1, discoveries.get());
        assertEquals(baseline + phase2, breaker.getUsed());
        assertEquals(phase2, run.held());
    }

    public void testFragmentWorkTripsBeforeDiscoveryAndLeavesLedgerAtZero() {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1b");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        FragmentExec fragment = new FragmentExec(relation(resolvedFiles(3), Map.of()));
        assertFalse(ComputeService.canSkipSplitDiscovery(fragment, service.formatReaderRegistry()));
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, run, done);
        CircuitBreakingException broke = expectThrows(CircuitBreakingException.class, () -> done.actionGet(30, TimeUnit.SECONDS));

        assertThat(broke.getMessage(), containsString(EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL));
        assertEquals(0, discoveries.get());
        assertEquals(0L, run.held());
        assertEquals(baseline, breaker.getUsed());
    }

    public void testSkipDiscoveryDoesNotCharge() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 99_000L);
        ExternalRelation external = relation(resolvedFiles(4), stats);
        Alias countAlias = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        FragmentExec fragment = new FragmentExec(new Aggregate(Source.EMPTY, external, List.of(), List.of(countAlias)));
        assertTrue(ComputeService.canSkipSplitDiscovery(fragment, service.formatReaderRegistry()));
        ExternalPlanningReservation.Run run = bind(info, breaker).openRun();
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, run, done);

        done.actionGet(30, TimeUnit.SECONDS);
        assertEquals(0, discoveries.get());
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, run.held());
    }

    /**
     * Listing and phase 2 admit on the one reservation {@code EsqlSession.execute} binds. Closing the
     * phase-2 run leaves the listing charge; a second run does not keep the first run's bytes.
     */
    public void testSeam1AndSeam2ShareLedgerAndReleaseTogether() throws Exception {
        String glob = "s3://bucket/data/*.parquet";
        String file1 = "s3://bucket/data/f1.parquet";
        String file2 = "s3://bucket/data/f2.parquet";
        List<Attribute> schema = List.of(referenceAttribute("id", DataType.INTEGER));
        Map<String, List<Attribute>> schemas = Map.of(file1, schema, file2, schema);
        Map<String, List<StorageEntry>> listings = Map.of(
            "s3://bucket/data/",
            List.of(
                new StorageEntry(StoragePath.of(file1), 100, Instant.EPOCH),
                new StorageEntry(StoragePath.of(file2), 200, Instant.EPOCH)
            )
        );
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        AtomicInteger discoveries = new AtomicInteger();
        ComputeService service = service(breaker, discoveries);
        ExternalSourceResolver resolver = listingResolver(schemas, listings, breaker);
        EsqlExecutionInfo info = executionInfo();
        ExternalPlanningReservation reservation = bind(info, breaker);
        resolver.planning(reservation);

        PlainActionFuture<ExternalSourceResolution> resolved = new PlainActionFuture<>();
        resolver.resolve(List.of(glob), Map.of(glob, new HashMap<>(Map.of("schema_resolution", "union_by_name"))), resolved);
        FileList listing = resolved.actionGet(30, TimeUnit.SECONDS).resolvedSource(glob).fileList();
        long seam1 = ExternalSourceResolver.listingPlanningCharge(listing);
        assertThat(seam1, greaterThan(0L));
        assertEquals(seam1, reservation.queryHeld());
        assertEquals(baseline + seam1, breaker.getUsed());

        ExternalSourceExec exec = relation(listing, Map.of()).toPhysicalExec();
        ExternalPlanningReservation.Run first = reservation.openRun();
        PlainActionFuture<ComputeService.CollectedSplits> phase2 = new PlainActionFuture<>();
        service.startPhase2OrSkip(exec, configuration(), info, () -> false, first, phase2);
        phase2.actionGet(30, TimeUnit.SECONDS);

        long seam2 = Phase2Reservation.bytesFor(exec.output(), listing);
        assertEquals(listing.fileCount() * Phase2Reservation.SHELL_BYTES, seam2);
        assertEquals(1, discoveries.get());
        assertEquals(seam2, first.held());
        assertEquals(seam1, reservation.queryHeld());
        assertEquals(baseline + seam1 + seam2, breaker.getUsed());

        first.close();
        assertEquals(0L, first.held());
        assertEquals(seam1, reservation.queryHeld());
        assertEquals(baseline + seam1, breaker.getUsed());

        ExternalPlanningReservation.Run second = reservation.openRun();
        PlainActionFuture<ComputeService.CollectedSplits> again = new PlainActionFuture<>();
        service.startPhase2OrSkip(exec, configuration(), info, () -> false, second, again);
        again.actionGet(30, TimeUnit.SECONDS);
        assertEquals(seam2, second.held());
        assertEquals(baseline + seam1 + seam2, breaker.getUsed());

        TransportEsqlQueryAction.releaseExternalPlanningBytes(info);
        assertEquals(0L, reservation.queryHeld());
        assertEquals(0L, second.held());
        assertEquals(baseline, breaker.getUsed());
    }

    /** No hive columns and no {@code _file.*} metadata: the survivor map is {@code Map.of()}, so only shells are billed. */
    public void testNoRetainedKeysBillsShellsOnly() {
        FileList files = resolvedFiles(4);
        ExternalRelation external = relation(files, Map.of());
        long charge = Phase2Reservation.bytesFor(external.output(), files);
        assertEquals(4 * Phase2Reservation.SHELL_BYTES, charge);
        assertNotEquals(4 * 1160L, charge);
    }

    /** Two files, one shared row, one projected hive column: one map, plus a shell per file. */
    public void testSharedHiveColumnBillsOneMap() {
        PartitionMetadata shared = hiveColumns(1, 2, true);
        FileList files = resolvedFiles(2, shared);
        List<Attribute> output = List.of(referenceAttribute("k0", DataType.INTEGER));
        assertEquals(Phase2Reservation.SHELL_BYTES * 2 + Phase2Reservation.perMap(1), Phase2Reservation.bytesFor(output, files));
    }

    /** Twenty unshared hive columns exceed the old flat 1160 bytes per file. */
    public void testDeepUnsharedLayoutExceedsFlatAllowance() {
        int files = 4;
        long charge = Phase2Reservation.bytesFor(hiveOutput(20), resolvedFiles(files, hiveColumns(20, files, false)));
        assertThat(charge, greaterThan(1160L * files));
    }

    /** Twenty hive columns shared as one row bill that one map plus shells, under the old per-file allowance. */
    public void testDeepSharedLayoutBillsOneMapPlusShells() {
        int files = 30;
        long charge = Phase2Reservation.bytesFor(hiveOutput(20), resolvedFiles(files, hiveColumns(20, files, true)));
        assertEquals(Phase2Reservation.perMap(20) + Phase2Reservation.SHELL_BYTES * files, charge);
        assertTrue(charge < 1160L * files);
    }

    /**
     * Detect, compact, and discover with the same retained keys the charge uses. One billed partition row is one
     * shared directory tuple: a dictionary listing with one file per directory, and a directory-grouped listing
     * with many files per directory.
     */
    public void testPhase2BytesMatchDiscoveredDirectoryTuples() {
        assertChargeMatchesDiscoveredTuples(manyFilesPerDirectory(), "DirectoryGroupedFileList");
        assertChargeMatchesDiscoveredTuples(oneFilePerDirectory(), "DictionaryFileList");
    }

    private static void assertChargeMatchesDiscoveredTuples(List<StorageEntry> entries, String encoding) {
        String base = "s3://bucket/data/";
        PartitionMetadata detected = HivePartitionDetector.INSTANCE.detect(entries, warning -> {});
        FileList compact = GlobExpander.compact(GlobExpander.fileListOf(entries, base + "**/*.parquet", detected), base);
        assertEquals(encoding, compact.getClass().getSimpleName());
        PartitionMetadata metadata = compact.partitionMetadata();
        List<Attribute> output = new ArrayList<>();
        for (Map.Entry<String, DataType> column : metadata.partitionColumns().entrySet()) {
            output.add(referenceAttribute(column.getKey(), column.getValue()));
        }
        output.add(new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.NAME, DataType.KEYWORD));
        ExternalSchema schema = ExternalSchema.dataAttributesOf(output);
        Set<String> metadataNames = ExternalMetadataColumns.metadataNames(output);
        Set<String> retained = PartitionValueLayout.retainedKeys(schema, metadata, metadataNames);
        SplitDiscoveryContext context = new SplitDiscoveryContext(
            null,
            compact,
            Map.of(),
            Map.of(),
            metadata,
            List.of(),
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE,
            metadataNames,
            retained
        );
        List<ExternalSplit> splits = new FileSplitProvider().discoverSplits(context).splits();
        Set<Object> tuples = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ExternalSplit split : splits) {
            tuples.add(((FileSplit) split).directoryTuple());
        }
        assertEquals(metadata.rowCount(), tuples.size());
        int files = compact.fileCount();
        int directories = metadata.rowCount() < files ? metadata.rowCount() : files;
        PartitionValueLayout layout = PartitionValueLayout.of(retained, metadata);
        long expected = Phase2Reservation.perMap(layout.directoryKeys().size()) * directories + Phase2Reservation.perMap(
            layout.perFileKeys().size()
        ) * files + Phase2Reservation.SHELL_BYTES * files;
        if (layout.directoryKeys().isEmpty() == false && layout.perFileKeys().isEmpty() == false) {
            expected += Phase2Reservation.VIEW_BYTES * files;
        }
        assertEquals(expected, Phase2Reservation.bytesFor(output, compact));
    }

    /** Many files in a few hive directories. Compaction keeps the directory-grouped encoding. */
    private static List<StorageEntry> manyFilesPerDirectory() {
        String base = "s3://bucket/data/";
        List<StorageEntry> entries = new ArrayList<>();
        int fileId = 0;
        for (int year = 2023; year <= 2024; year++) {
            for (int month = 1; month <= 12; month++) {
                for (int part = 0; part < 20; part++) {
                    entries.add(entry(base + "year=" + year + "/month=" + month + "/part-" + (fileId++) + ".parquet"));
                }
            }
        }
        return entries;
    }

    /** One file per hive directory. Compaction keeps the dictionary encoding and one row per file. */
    private static List<StorageEntry> oneFilePerDirectory() {
        String base = "s3://bucket/data/";
        List<StorageEntry> entries = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            for (int day = 1; day <= 28; day++) {
                entries.add(entry(base + "year=2024/month=" + month + "/day=" + day + "/data.parquet"));
            }
        }
        return entries;
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }

    /**
     * Location keys are derived at read, so a bound path does not grow the charge and path length is never read.
     * Size and hive keys still bill a map. Hive plus size and modified bills both layers.
     */
    public void testPhase2ChargeFollowsRetainedKeysNotPathLength() {
        List<Attribute> dataOnly = List.of(referenceAttribute("x", DataType.INTEGER));
        assertEquals(2 * Phase2Reservation.SHELL_BYTES, Phase2Reservation.bytesFor(dataOnly, resolvedFiles(2)));
        assertEquals(0L, Phase2Reservation.bytesFor(dataOnly, FileList.UNRESOLVED));
        assertEquals(0L, Phase2Reservation.bytesFor(dataOnly, FileList.EMPTY));

        StoragePath shortPath = StoragePath.of("s3://b/a.parquet");
        StoragePath longPath = StoragePath.of("s3://b/" + "p".repeat(4000) + "/a.parquet");
        List<Attribute> locationBound = List.of(
            referenceAttribute("x", DataType.INTEGER),
            new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.PATH, DataType.KEYWORD),
            new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.NAME, DataType.KEYWORD),
            new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.DIRECTORY, DataType.KEYWORD)
        );
        assertEquals(Phase2Reservation.SHELL_BYTES, Phase2Reservation.bytesFor(locationBound, oneFile(shortPath, null)));
        assertEquals(Phase2Reservation.SHELL_BYTES, Phase2Reservation.bytesFor(locationBound, oneFile(longPath, null)));

        List<Attribute> sizeBound = List.of(new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.SIZE, DataType.LONG));
        assertEquals(
            Phase2Reservation.SHELL_BYTES + Phase2Reservation.perMap(1),
            Phase2Reservation.bytesFor(sizeBound, oneFile(shortPath, null))
        );

        StoragePath hivePath = StoragePath.of("s3://b/year=2024/a.parquet");
        PartitionMetadata partitions = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(hivePath, Map.of("year", 2024)));
        List<Attribute> hiveBound = List.of(referenceAttribute("year", DataType.INTEGER));
        assertEquals(
            Phase2Reservation.SHELL_BYTES + Phase2Reservation.perMap(1),
            Phase2Reservation.bytesFor(hiveBound, oneFile(hivePath, partitions))
        );

        List<Attribute> hiveAndSize = List.of(
            referenceAttribute("year", DataType.INTEGER),
            new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.SIZE, DataType.LONG),
            new ExternalMetadataAttribute(Source.EMPTY, FileMetadataColumns.MODIFIED, DataType.DATETIME)
        );
        long bothLayers = Phase2Reservation.SHELL_BYTES + Phase2Reservation.perMap(1) + Phase2Reservation.perMap(2)
            + Phase2Reservation.VIEW_BYTES;
        assertEquals(bothLayers, Phase2Reservation.bytesFor(hiveAndSize, oneFile(hivePath, partitions)));
    }

    public void testReleaseReturnsSuccessAndFailureToBaseline() {
        CircuitBreaker breaker = requestBreaker("1mb");
        long baseline = breaker.getUsed();

        EsqlExecutionInfo success = executionInfo();
        ExternalPlanningReservation successReservation = bind(success, breaker);
        successReservation.chargeQuery(400);
        successReservation.openRun().charge(50);
        TransportEsqlQueryAction.releaseExternalPlanningBytes(success);
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, successReservation.queryHeld());

        EsqlExecutionInfo failure = executionInfo();
        ExternalPlanningReservation failureReservation = bind(failure, breaker);
        failureReservation.chargeQuery(250);
        TransportEsqlQueryAction.releaseExternalPlanningBytes(failure);
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, failureReservation.queryHeld());

        TransportEsqlQueryAction.releaseExternalPlanningBytes(failure);
        assertEquals(baseline, breaker.getUsed());
    }

    private static ExternalPlanningReservation bind(EsqlExecutionInfo info, CircuitBreaker breaker) {
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        info.externalPlanning(reservation);
        return reservation;
    }

    private static ComputeService service(CircuitBreaker breaker, AtomicInteger discoveries) {
        return service(breaker, ctx -> {
            discoveries.incrementAndGet();
            return SplitDiscoveryResult.EMPTY;
        });
    }

    private static ComputeService service(CircuitBreaker breaker, SplitProvider splitter) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(
            mock(org.elasticsearch.threadpool.Scheduler.Cancellable.class)
        );

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(EsqlPlugin.GROK_WATCHDOG_MAX_EXECUTION_TIME);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, registered);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        ExchangeService exchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        FormatReaderRegistry readers = parquetRegistry();
        ExternalSourceFactory files = new ExternalSourceFactory() {
            @Override
            public String type() {
                return "parquet";
            }

            @Override
            public boolean canHandle(String location) {
                return false;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public SplitProvider splitProvider() {
                return splitter;
            }
        };
        OperatorFactoryRegistry operators = new OperatorFactoryRegistry(
            Map.of("parquet", files),
            Map.of(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        TransportActionServices transport = new TransportActionServices(
            transportService,
            null,
            exchangeService,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            mock(PlannerSettings.Holder.class),
            null
        );
        return new ComputeService(transport, null, null, threadPool, BigArrays.NON_RECYCLING_INSTANCE, blockFactory, operators, readers);
    }

    private static FormatReaderRegistry parquetRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        AggregatePushdownSupport support = (aggregates, groupings) -> {
            if (groupings.isEmpty() == false) {
                return AggregatePushdownSupport.Pushability.NO;
            }
            for (Expression agg : aggregates) {
                if (agg instanceof Count == false) {
                    return AggregatePushdownSupport.Pushability.NO;
                }
            }
            return AggregatePushdownSupport.Pushability.YES;
        };
        registry.registerLazy("parquet", (settings, blockFactory) -> new CountingReader(support), null, null);
        return registry;
    }

    private static ExternalRelation relation(FileList files, Map<String, Object> stats) {
        List<Attribute> attrs = List.of(referenceAttribute("x", DataType.INTEGER));
        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attrs;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "file:///data/*.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return stats;
            }
        };
        return new ExternalRelation(Source.EMPTY, "file:///data/*.parquet", metadata, attrs, files, Map.of());
    }

    /** A listing that reports itself a prefix of its dataset, which is what stands the pre-charge down. */
    private static FileList truncatedFiles(int count) {
        List<StorageEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(new StorageEntry(StoragePath.of("file:///f" + i + ".parquet"), 1000L, Instant.EPOCH));
        }
        return GlobExpander.truncatedFileListOf(entries, "file:///*.parquet");
    }

    private static FileList resolvedFiles(int count) {
        return resolvedFiles(count, null);
    }

    private static FileList resolvedFiles(int count, PartitionMetadata metadata) {
        return new FileList() {
            @Override
            public int fileCount() {
                return count;
            }

            @Override
            public StoragePath path(int i) {
                return StoragePath.of("file:///f" + i + ".parquet");
            }

            @Override
            public long size(int i) {
                return 1L;
            }

            @Override
            public long lastModifiedMillis(int i) {
                return 0L;
            }

            @Override
            public String originalPattern() {
                return "file:///*.parquet";
            }

            @Override
            public PartitionMetadata partitionMetadata() {
                return metadata;
            }

            @Override
            public boolean isResolved() {
                return true;
            }

            @Override
            public boolean isEmpty() {
                return count == 0;
            }

            @Override
            public long estimatedBytes() {
                return 0L;
            }
        };
    }

    private static PartitionMetadata hiveColumns(int columns, int files, boolean shareOneRow) {
        LinkedHashMap<String, DataType> types = new LinkedHashMap<>();
        Object[][] values = new Object[columns][];
        for (int c = 0; c < columns; c++) {
            types.put("k" + c, DataType.INTEGER);
            Object[] column = new Object[files];
            Arrays.fill(column, c);
            values[c] = column;
        }
        PartitionMetadata raw = PartitionMetadata.columnar(types, values, files);
        if (shareOneRow == false) {
            return raw;
        }
        short[] groups = new short[files];
        return raw.shareByGroups(groups, 1);
    }

    private static List<Attribute> hiveOutput(int columns) {
        List<Attribute> output = new java.util.ArrayList<>(columns);
        for (int c = 0; c < columns; c++) {
            output.add(referenceAttribute("k" + c, DataType.INTEGER));
        }
        return output;
    }

    private static FileList oneFile(StoragePath path, PartitionMetadata partitions) {
        return new FileList() {
            @Override
            public int fileCount() {
                return 1;
            }

            @Override
            public StoragePath path(int i) {
                return path;
            }

            @Override
            public long size(int i) {
                return 1L;
            }

            @Override
            public long lastModifiedMillis(int i) {
                return 0L;
            }

            @Override
            public String originalPattern() {
                return path.toString();
            }

            @Override
            public PartitionMetadata partitionMetadata() {
                return partitions;
            }

            @Override
            public boolean isResolved() {
                return true;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public long estimatedBytes() {
                return path.toString().length();
            }
        };
    }

    private static Configuration configuration() {
        return new Configuration(
            java.time.Instant.EPOCH,
            Locale.ROOT,
            "test",
            "test",
            QueryPragmas.EMPTY,
            1000,
            1000,
            null,
            false,
            Map.of(),
            0L,
            false,
            1000,
            1000,
            ResolvedSettings.EMPTY,
            Map.of()
        );
    }

    private static EsqlExecutionInfo executionInfo() {
        return new EsqlExecutionInfo(Predicates.always(), EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
    }

    private static ExternalSourceResolver listingResolver(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        CircuitBreaker breaker
    ) {
        BlockFactory factory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        ListingStorage storage = new ListingStorage(listingsByPrefix);
        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("s3");
            }

            @Override
            public Set<FormatSpec> formatSpecs() {
                return Set.of(FormatSpec.of("parquet", ".parquet"));
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", new StorageProviderFactory() {
                    @Override
                    public StorageProvider create(Settings settings) {
                        return storage;
                    }

                    @Override
                    public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                        if (config == null || config.isEmpty()) {
                            return Configured.empty(storage);
                        }
                        return new Configured<>(storage, Set.copyOf(config.keySet()));
                    }
                });
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> new ListingReader(schemasByPath));
            }
        };
        List<DataSourcePlugin> plugins = List.of(plugin);
        DataSourceModule module = new DataSourceModule(
            plugins,
            DataSourceCapabilities.build(plugins),
            Settings.EMPTY,
            factory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(mock(EncryptionService.class)),
            () -> false
        );
        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
    }

    private static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(CircuitBreakerMetrics.NOOP, settings, List.of(), clusterSettings).getBreaker(
            CircuitBreaker.REQUEST
        );
    }

    private static final class CountingReader implements NoConfigFormatReader {
        private final AggregatePushdownSupport support;

        CountingReader(AggregatePushdownSupport support) {
            this.support = support;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public AggregatePushdownSupport aggregatePushdownSupport() {
            return support;
        }

        @Override
        public void close() {}
    }

    private static final class ListingReader implements NoConfigFormatReader {
        private final Map<String, List<Attribute>> schemasByPath;

        ListingReader(Map<String, List<Attribute>> schemasByPath) {
            this.schemasByPath = schemasByPath;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            List<Attribute> schema = schemasByPath.get(object.path().toString());
            if (schema == null) {
                throw new IllegalArgumentException("No schema configured for path: " + object.path());
            }
            return new SimpleSourceMetadata(schema, "parquet", object.path().toString());
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static final class ListingStorage implements StorageProvider {
        private final Map<String, List<StorageEntry>> listingsByPrefix;

        ListingStorage(Map<String, List<StorageEntry>> listingsByPrefix) {
            this.listingsByPrefix = listingsByPrefix;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return object(path, 0);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return object(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return object(path, length);
        }

        private static StorageObject object(StoragePath path, long length) {
            return new StorageObject() {
                @Override
                public StorageIdentity storageIdentity() {
                    return AbstractTestStorageObject.NOOP;
                }

                @Override
                public InputStream newStream() {
                    return InputStream.nullInputStream();
                }

                @Override
                public InputStream newStream(long position, long range) {
                    return InputStream.nullInputStream();
                }

                @Override
                public long length() {
                    return length;
                }

                @Override
                public Instant lastModified() {
                    return Instant.EPOCH;
                }

                @Override
                public boolean exists() {
                    return true;
                }

                @Override
                public StoragePath path() {
                    return path;
                }
            };
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            List<StorageEntry> entries = listingsByPrefix.getOrDefault(prefix.toString(), List.of());
            Iterator<StorageEntry> it = entries.iterator();
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    if (it.hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }
}
