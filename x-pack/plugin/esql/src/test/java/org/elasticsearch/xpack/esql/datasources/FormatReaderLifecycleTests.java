/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the {@link FormatReader} lifecycle contract at the framework's minting sites: a reader an SPI
 * {@code with*} call hands back is owned by the caller and gets closed, while the registry's shared instance
 * never does.
 * <p>
 * The reader here mints a distinct, self-identifying instance for every {@code with*} call that applies and
 * records each close, so a test can assert exactly which instances of a derivation chain were released — the
 * one thing a spy on a single object cannot express, since the whole point of the contract is that the
 * instances are different objects with different owners.
 */
public class FormatReaderLifecycleTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final Map<String, Object> CONFIG = Map.of("delimiter", ";");

    // === Population A: planning / validation-scoped readers, closed where they were minted ===

    public void testValidateConfigClosesTheReaderItConfigured() {
        TrackingFormatReader root = new TrackingFormatReader();
        newFileSourceFactory(root).validateConfig("s3://bucket/data.test", CONFIG);

        assertEquals("withConfig minted exactly one instance", 1, root.minted().size());
        assertEquals("the configured instance is released with the validation that minted it", root.minted(), root.closed());
    }

    public void testValidateConfigClosesTheReaderWhenValidationRejectsAnUnknownKey() {
        // The reader claims only what it recognises, so `bogus` reaches ConfigKeyValidator unclaimed and
        // validation throws — after the reader has already been minted.
        TrackingFormatReader root = new TrackingFormatReader();
        Map<String, Object> config = Map.of("delimiter", ";", "bogus", "x");

        expectThrows(IllegalArgumentException.class, () -> newFileSourceFactory(root).validateConfig("s3://bucket/data.test", config));

        assertEquals(1, root.minted().size());
        assertEquals("the failure path releases it too", root.minted(), root.closed());
    }

    public void testResolveMetadataClosesTheReaderItConfigured() {
        TrackingFormatReader root = new TrackingFormatReader();
        SourceMetadata metadata = newFileSourceFactory(root).resolveMetadata("s3://bucket/data.test", CONFIG);

        assertNotNull(metadata);
        assertEquals(1, root.minted().size());
        assertEquals(root.minted(), root.closed());
    }

    public void testResolveMetadataClosesTheReaderWhenTheMetadataReadFails() {
        TrackingFormatReader root = new TrackingFormatReader().failMetadataWith(new IOException("boom"));

        expectThrows(IllegalArgumentException.class, () -> newFileSourceFactory(root).resolveMetadata("s3://bucket/data.test", CONFIG));

        assertEquals(1, root.minted().size());
        assertEquals("a failed metadata read still releases the reader", root.minted(), root.closed());
    }

    public void testResolveMetadataAsyncClosesTheReaderOnlyAfterTheListenerCompletes() {
        // The async read outlives the method, so the release has to ride the completion listener rather than
        // the return. The reader snapshots what had been closed at the moment the read was entered; the
        // instance being read through must not be in it.
        TrackingFormatReader root = new TrackingFormatReader();
        AtomicReference<SourceMetadata> response = new AtomicReference<>();

        newFileSourceFactory(root).resolveMetadataAsync(
            "s3://bucket/data.test",
            null,
            CONFIG,
            Runnable::run,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertNotNull(response.get());
        // Two mints: the up-front validateConfig pass, which releases its own, then the reader this method
        // actually reads through.
        List<TrackingFormatReader> minted = root.minted();
        assertEquals(2, minted.size());
        assertFalse("still open while the read was in flight", root.closedDuringMetadata().contains(minted.get(1)));
        assertEveryMintedInstanceReleased(root);
    }

    public void testResolveMetadataAsyncClosesTheReaderWhenTheReadFails() {
        TrackingFormatReader root = new TrackingFormatReader().failMetadataWith(new IOException("boom"));
        AtomicReference<Exception> failure = new AtomicReference<>();

        newFileSourceFactory(root).resolveMetadataAsync(
            "s3://bucket/data.test",
            null,
            CONFIG,
            Runnable::run,
            ActionListener.wrap(m -> fail("expected a failure"), failure::set)
        );

        assertNotNull(failure.get());
        assertEveryMintedInstanceReleased(root);
    }

    /**
     * The other half of the contract: when no {@code with*} call applies, the reader the caller holds is the
     * registry's node-level singleton and nothing may close it. Closing it once per query is exactly the
     * regression the identity guard exists to prevent.
     */
    public void testTheRegistryInstanceIsNeverClosed() {
        TrackingFormatReader root = new TrackingFormatReader();
        FileSourceFactory factory = newFileSourceFactory(root);

        factory.validateConfig("s3://bucket/data.test", Map.of());
        factory.resolveMetadata("s3://bucket/data.test", Map.of());
        factory.operatorFactory()
            .create(
                SourceOperatorContext.builder()
                    .sourceType("file")
                    .path(StoragePath.of("s3://bucket/data.test"))
                    .executor(Runnable::run)
                    .build()
            );

        assertEquals("no with* call applied, so nothing was minted", List.of(), root.minted());
        assertEquals("and nothing was closed", List.of(), root.closed());
    }

    /**
     * Split discovery mints a config-aware reader per file to ask which record splitter and which byte ranges the
     * format supports, then discards it. Nothing past planning reads through those, so they have to go where they
     * were minted rather than ride anything downstream.
     */
    public void testSplitDiscoveryClosesThePerFileProbeReaders() {
        TrackingFormatReader root = new TrackingFormatReader();
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("test-format", (s, bf) -> root, Settings.EMPTY, null);
        formatRegistry.registerExtension(".test", "test-format");

        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(StubStorageProvider::new));

        FileSplitProvider splitProvider = new FileSplitProvider(
            1024,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY,
            null
        );

        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://bucket/data/f1.test"), 4096, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/data/f2.test"), 4096, Instant.EPOCH)
            ),
            "s3://bucket/data/*.test"
        );

        splitProvider.discoverSplits(new SplitDiscoveryContext(null, fileList, CONFIG, PartitionMetadata.EMPTY, List.of()));

        // Two probes per file: the splitter/segment probe and the range-split probe, each config-aware and each
        // minting its own instance.
        assertEquals(4, root.minted().size());
        assertEveryMintedInstanceReleased(root);
    }

    // === Population B: the per-query execution reader, released through the operator factory's onClose ===

    public void testOperatorFactoryReleasesTheConfiguredReaderWhenTheLastOperatorFinishes() {
        TrackingFormatReader root = new TrackingFormatReader();
        AsyncExternalSourceOperatorFactory factory = executionFactory(root);
        assertEquals("the with* chain minted the query's reader", 1, root.minted().size());
        assertEquals("held open while the operator can still run", List.of(), root.closed());

        drainToCompletion(factory);

        // Two instances by now: the query-scoped one the factory built, plus the per-read re-mint the
        // single-file rail stamps with this file's read config.
        assertEquals(2, root.minted().size());
        assertEveryMintedInstanceReleased(root);
    }

    public void testOperatorFactoryReleasesTheConfiguredReaderWhenTheReadFails() {
        TrackingFormatReader root = new TrackingFormatReader().failReadWith(new IOException("read blew up"));
        AsyncExternalSourceOperatorFactory factory = executionFactory(root);

        DriverContext driverContext = driverContext();
        SourceOperator operator = factory.get(driverContext);
        expectThrows(Exception.class, () -> {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    page.releaseBlocks();
                }
            }
        });
        operator.close();

        assertFalse("the read got far enough to mint", root.minted().isEmpty());
        assertEveryMintedInstanceReleased(root);
    }

    /**
     * Cancellation shape: the driver closes the source operator before the producer has drained, which finishes
     * the buffer and takes the producer loop down the same {@code DrainResult.DONE} path a task cancellation
     * does. The reader must be released there too, not only on the clean-EOF path.
     */
    public void testOperatorFactoryReleasesTheConfiguredReaderWhenTheOperatorIsClosedEarly() {
        TrackingFormatReader root = new TrackingFormatReader();
        QueuedExecutor executor = new QueuedExecutor();
        AsyncExternalSourceOperatorFactory factory = executionFactory(root, executor);

        SourceOperator operator = factory.get(driverContext());
        assertEquals("the producer has not run yet", List.of(), root.closed());
        operator.close();
        executor.runAll();

        assertEveryMintedInstanceReleased(root);
    }

    /**
     * Deferred extraction keeps the source's resources alive past the last source operator, because an
     * {@code ExternalFieldExtractOperator} still reads through them. The reader rides the same {@code onClose}
     * chain as the storage lease, so it must inherit that extension rather than be released at source EOF.
     */
    public void testDeferredExtractionHoldsTheReaderOpenUntilTheExtractorRegistryCloses() throws IOException {
        TrackingFormatReader root = new TrackingFormatReader();
        AsyncExternalSourceOperatorFactory factory = executionFactory(root, Runnable::run, true);

        DriverContext driverContext = driverContext();
        SourceExtractors registry = factory.sourceExtractorsFor(driverContext);
        drainToCompletion(factory, driverContext);

        // minted[0] is the query-scoped reader on the onClose chain; minted[1] is the per-read re-mint, whose
        // life is its iterator's and which is therefore already gone.
        List<TrackingFormatReader> minted = root.minted();
        assertEquals(2, minted.size());
        assertEquals("the query's reader is held open for the extract operator still to run", List.of(minted.get(1)), root.closed());

        registry.close();
        assertEveryMintedInstanceReleased(root);
    }

    /**
     * A {@code with*} chain hands the caller a new instance per applied setting, but only the tail is ever read
     * through — the ones before it were configured and dropped. The contract says {@code with*} acquires
     * nothing, so releasing the tail is the whole obligation; this pins that the framework does exactly that
     * rather than quietly relying on a chain of length one.
     */
    public void testOnlyTheTailOfTheWithChainIsReleased() {
        TrackingFormatReader root = new TrackingFormatReader();
        // Non-empty attributes make withSchema apply as well, so the chain is withConfig -> withSchema.
        AsyncExternalSourceOperatorFactory factory = executionFactory(root, Runnable::run, false, List.of(field("value")));

        assertEquals("two settings applied, two instances minted", 2, root.minted().size());
        TrackingFormatReader intermediate = root.minted().get(0);
        TrackingFormatReader tail = root.minted().get(1);
        drainToCompletion(factory);

        assertTrue("the tail is the instance the read went through", root.closed().contains(tail));
        assertFalse(
            "the intermediate was configured and dropped, never read through, so by contract it holds nothing",
            root.closed().contains(intermediate)
        );
    }

    // === Population C: per-file re-mints during execution ===

    /**
     * The multi-file producer re-mints a reader per file ({@code withReadConfig} stamps how that file is being
     * read). Each of those is the factory's own, and each has to go when its file's iterator does — while the
     * shared instance every file derives from stays open for the files still to come.
     */
    public void testPerFileRemintsAreEachReleasedAndTheSharedReaderIsNot() {
        TrackingFormatReader shared = new TrackingFormatReader();
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.test"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.test"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.test"), 300, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.test");

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            shared,
            StoragePath.of("s3://bucket/data/f1.test"),
            List.of(),
            100,
            10,
            (Executor) Runnable::run
        ).fileList(fileList).build();

        drainToCompletion(factory);

        assertEquals("one re-mint per file", 3, shared.minted().size());
        assertEveryMintedInstanceReleased(shared);
    }

    // === helpers ===

    /**
     * Every instance a {@code with*} call handed out was released exactly once (a double close would fail the
     * size check {@code containsInAnyOrder} makes), and the registry's shared instance was left alone.
     */
    private static void assertEveryMintedInstanceReleased(TrackingFormatReader root) {
        assertThat("every minted instance released, exactly once", root.closed(), containsInAnyOrder(root.minted().toArray()));
        assertFalse("the registry's shared instance is never closed", root.closed().contains(root));
    }

    private static FileSourceFactory newFileSourceFactory(TrackingFormatReader reader) {
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("test-format", (s, bf) -> reader, Settings.EMPTY, null);
        formatRegistry.registerExtension(".test", "test-format");

        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(StubStorageProvider::new));

        return new FileSourceFactory(storageRegistry, formatRegistry, new DecompressionCodecRegistry(), Settings.EMPTY);
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(TrackingFormatReader reader) {
        return executionFactory(reader, Runnable::run);
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(TrackingFormatReader reader, Executor executor) {
        return executionFactory(reader, executor, false);
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(
        TrackingFormatReader reader,
        Executor executor,
        boolean deferredExtraction
    ) {
        return executionFactory(reader, executor, deferredExtraction, List.of());
    }

    /** Builds the production chain: {@link FileSourceFactory#operatorFactory()} mints the reader and owns it. */
    private static AsyncExternalSourceOperatorFactory executionFactory(
        TrackingFormatReader reader,
        Executor executor,
        boolean deferredExtraction,
        List<Attribute> attributes
    ) {
        StoragePath path = StoragePath.of("s3://bucket/data.test");
        SourceOperator.SourceOperatorFactory built = newFileSourceFactory(reader).operatorFactory()
            .create(
                SourceOperatorContext.builder()
                    .sourceType("file")
                    .path(path)
                    .executor(executor)
                    .config(CONFIG)
                    .attributes(attributes)
                    .deferredExtraction(deferredExtraction)
                    .build()
            );
        return (AsyncExternalSourceOperatorFactory) built;
    }

    private static DriverContext driverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        return driverContext;
    }

    private static void drainToCompletion(AsyncExternalSourceOperatorFactory factory) {
        drainToCompletion(factory, driverContext());
    }

    private static void drainToCompletion(AsyncExternalSourceOperatorFactory factory, DriverContext driverContext) {
        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
            operator.close();
        }
    }

    private static Attribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    /** Defers every task until {@link #runAll()}, so a test can interleave a close with the producer loop. */
    private static final class QueuedExecutor implements Executor {
        private final Deque<Runnable> queued = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            queued.add(command);
        }

        void runAll() {
            while (queued.isEmpty() == false) {
                queued.poll().run();
            }
        }
    }

    /**
     * Mints a fresh instance for every {@code with*} call that applies and records every close, against lists
     * shared by the whole lineage so the root is the single handle a test needs.
     * <p>
     * Deliberately not a Mockito spy: the contract is about which <em>instance</em> of a chain gets closed, and
     * a spy on one object cannot tell the parent's close from the child's.
     * <p>
     * Declares {@link ColumnExtractorAware} because that marker is one of the two signals
     * {@code FileSourceFactory} requires before it enables deferred extraction, which is the lifetime-extension
     * case under test. With no {@code _rowPosition} in the projection the runtime producer handshake never runs,
     * so the marker alone is enough and the iterators below stay plain.
     */
    private static final class TrackingFormatReader implements FormatReader, ColumnExtractorAware {

        private final TrackingFormatReader root;
        private final String label;
        private final List<TrackingFormatReader> minted;
        private final List<TrackingFormatReader> closed;
        /** Snapshot of {@link #closed} taken inside {@code metadata}, so a test can prove the reader was open during the read. */
        private volatile List<TrackingFormatReader> closedDuringMetadata;
        private IOException metadataFailure;
        private IOException readFailure;

        TrackingFormatReader() {
            this.root = this;
            this.label = "root";
            this.minted = new CopyOnWriteArrayList<>();
            this.closed = new CopyOnWriteArrayList<>();
        }

        private TrackingFormatReader(TrackingFormatReader parent, String setting) {
            this.root = parent.root;
            this.label = parent.label + "->" + setting;
            this.minted = parent.minted;
            this.closed = parent.closed;
            this.metadataFailure = parent.metadataFailure;
            this.readFailure = parent.readFailure;
        }

        TrackingFormatReader failMetadataWith(IOException failure) {
            this.metadataFailure = failure;
            return this;
        }

        TrackingFormatReader failReadWith(IOException failure) {
            this.readFailure = failure;
            return this;
        }

        /** Every instance the lineage minted, in mint order. Excludes the root, which nobody owns. */
        List<TrackingFormatReader> minted() {
            return List.copyOf(minted);
        }

        /** Every instance closed, in close order. */
        List<TrackingFormatReader> closed() {
            return List.copyOf(closed);
        }

        List<TrackingFormatReader> closedDuringMetadata() {
            return closedDuringMetadata == null ? List.of() : closedDuringMetadata;
        }

        private TrackingFormatReader mint(String setting) {
            TrackingFormatReader derived = new TrackingFormatReader(this, setting);
            minted.add(derived);
            return derived;
        }

        @Override
        public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
            if (config == null || config.isEmpty()) {
                return Configured.empty(this);
            }
            // Claim only the keys we know, so a test can drive the unknown-key rejection path.
            return Configured.fromKnownSubset(mint("config"), config, Set.of("delimiter"));
        }

        @Override
        public FormatReader withSchema(List<Attribute> schema) {
            return schema == null || schema.isEmpty() ? this : mint("schema");
        }

        @Override
        public FormatReader withReadConfig(String readConfig) {
            return readConfig == null ? this : mint("readConfig");
        }

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            closedDuringMetadata = List.copyOf(closed);
            if (metadataFailure != null) {
                throw metadataFailure;
            }
            return new SimpleSourceMetadata(List.of(), "file", object.path().toString());
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            if (readFailure != null) {
                throw readFailure;
            }
            return new CloseableIterator<>() {
                private boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return consumed == false;
                }

                @Override
                public Page next() {
                    if (consumed) {
                        throw new NoSuchElementException();
                    }
                    consumed = true;
                    return new Page(1);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-format";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".test");
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {
            closed.add(this);
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /** Minimal storage provider: hands back a stub object for any path. */
    private static final class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
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

    private static final class StubStorageObject implements StorageObject {
        private final StoragePath path;

        StubStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return 0;
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
    }
}
