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
import org.elasticsearch.compute.data.Block;
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
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that format-reader factories remain resource-free through planning and that every
 * metadata or execution operation owns the distinct runtime reader it creates.
 */
public class FormatReaderLifecycleTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final Map<String, Object> CONFIG = Map.of("delimiter", ";");

    public void testValidateConfigDoesNotBuildAReader() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();

        newFileSourceFactory(readers).validateConfig("s3://bucket/data.test", CONFIG);

        assertEquals(1, readers.inspectCount());
        assertEquals(List.of(CONFIG), readers.configurations());
        assertNoReadersBuilt(readers);
    }

    public void testValidateConfigRejectsAnUnknownKeyWithoutBuildingAReader() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        Map<String, Object> config = Map.of("delimiter", ";", "bogus", "x");

        expectThrows(IllegalArgumentException.class, () -> newFileSourceFactory(readers).validateConfig("s3://bucket/data.test", config));

        assertEquals(1, readers.inspectCount());
        assertEquals(List.of(config), readers.configurations());
        assertNoReadersBuilt(readers);
    }

    public void testResolveMetadataUsesAndClosesTheExactBuiltReader() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();

        SourceMetadata metadata = newFileSourceFactory(readers).resolveMetadata("s3://bucket/data.test", CONFIG);

        assertNotNull(metadata);
        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, onlyMetadataReader(readers));
        assertTrue("the reader must still be open while metadata is read", reader.wasOpenDuringMetadata());
        assertTrue(reader.isClosed());
        assertEquals(CONFIG, reader.config());
        assertAllBuiltReadersClosed(readers);
    }

    public void testResolveMetadataClosesTheBuiltReaderWhenMetadataFails() {
        TrackingFormatReaderFactory readers = TrackingFormatReaderFactory.failingMetadata(new IOException("boom"));

        expectThrows(IllegalArgumentException.class, () -> newFileSourceFactory(readers).resolveMetadata("s3://bucket/data.test", CONFIG));

        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, onlyMetadataReader(readers));
        assertTrue("the reader must still be open while metadata is read", reader.wasOpenDuringMetadata());
        assertTrue(reader.isClosed());
        assertAllBuiltReadersClosed(readers);
    }

    public void testResolveMetadataAsyncClosesTheBuiltReaderAfterTheListenerCompletes() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        AtomicReference<SourceMetadata> response = new AtomicReference<>();
        AtomicReference<TrackingFormatReader> listenerReader = new AtomicReference<>();
        AtomicReference<Boolean> closedInListener = new AtomicReference<>();

        newFileSourceFactory(readers).resolveMetadataAsync(
            "s3://bucket/data.test",
            null,
            CONFIG,
            Runnable::run,
            ActionListener.wrap(metadata -> {
                response.set(metadata);
                TrackingFormatReader reader = onlyMetadataReader(readers);
                listenerReader.set(reader);
                closedInListener.set(reader.isClosed());
            }, e -> fail("unexpected failure: " + e))
        );

        assertNotNull(response.get());
        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, listenerReader.get());
        assertFalse("the completion wrapper must close after invoking the listener", closedInListener.get());
        assertTrue("the reader must still be open while metadata is read", reader.wasOpenDuringMetadata());
        assertTrue(reader.isClosed());
        assertEquals("validation inspects the factory without creating a reader", 1, readers.inspectCount());
        assertEquals("validation must not create a runtime reader", 1, readers.builtReaders().size());
        assertAllBuiltReadersClosed(readers);
    }

    public void testResolveMetadataAsyncClosesTheBuiltReaderWhenMetadataFails() {
        TrackingFormatReaderFactory readers = TrackingFormatReaderFactory.failingMetadata(new IOException("boom"));
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<TrackingFormatReader> listenerReader = new AtomicReference<>();
        AtomicReference<Boolean> closedInListener = new AtomicReference<>();

        newFileSourceFactory(readers).resolveMetadataAsync(
            "s3://bucket/data.test",
            null,
            CONFIG,
            Runnable::run,
            ActionListener.wrap(metadata -> fail("expected a failure"), e -> {
                failure.set(e);
                TrackingFormatReader reader = onlyMetadataReader(readers);
                listenerReader.set(reader);
                closedInListener.set(reader.isClosed());
            })
        );

        assertNotNull(failure.get());
        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, listenerReader.get());
        assertFalse("the completion wrapper must close after invoking the listener", closedInListener.get());
        assertTrue("the reader must still be open while metadata is read", reader.wasOpenDuringMetadata());
        assertTrue(reader.isClosed());
        assertAllBuiltReadersClosed(readers);
    }

    public void testExecutionBuildsReadsAndClosesOneRuntimeReader() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        List<Attribute> attributes = List.of(field("value"));
        AsyncExternalSourceOperatorFactory factory = executionFactory(readers, Runnable::run, false, attributes);

        assertNoReadersBuilt(readers);

        drainToCompletion(factory);

        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, onlyReadReader(readers));
        assertEquals(CONFIG, reader.config());
        assertEquals(attributes, reader.schema());
        assertEquals(List.of("value"), reader.readContext().projectedColumns());
        assertNotNull(reader.readConfig());
        assertTrue(reader.isClosed());
        assertAllBuiltReadersClosed(readers);
    }

    public void testExecutionClosesTheBuiltReaderWhenReadFails() {
        TrackingFormatReaderFactory readers = TrackingFormatReaderFactory.failingRead(new IOException("read blew up"));
        AsyncExternalSourceOperatorFactory factory = executionFactory(readers);

        DriverContext driverContext = driverContext();
        SourceOperator operator = factory.get(driverContext);
        try {
            expectThrows(Exception.class, () -> {
                while (operator.isFinished() == false) {
                    Page page = operator.getOutput();
                    if (page != null) {
                        page.releaseBlocks();
                    }
                }
            });
        } finally {
            operator.close();
        }

        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, onlyReadReader(readers));
        assertTrue(reader.isClosed());
        assertAllBuiltReadersClosed(readers);
    }

    public void testClosingBeforeProducerStartDoesNotBuildAReader() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        QueuedExecutor executor = new QueuedExecutor();
        AsyncExternalSourceOperatorFactory factory = executionFactory(readers, executor);

        SourceOperator operator = factory.get(driverContext());
        assertNoReadersBuilt(readers);
        operator.close();
        executor.runAll();

        assertNoReadersBuilt(readers);
    }

    public void testMultiFileExecutionBuildsAndClosesOneDistinctReaderPerFile() {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.test"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.test"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.test"), 300, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.test");

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            readers,
            StoragePath.of("s3://bucket/data/f1.test"),
            List.of(),
            100,
            10,
            (Executor) Runnable::run
        ).fileList(fileList).build();

        drainToCompletion(factory);

        List<TrackingFormatReader> builtReaders = readers.builtReaders();
        assertEquals(3, builtReaders.size());
        assertNotSame(builtReaders.get(0), builtReaders.get(1));
        assertNotSame(builtReaders.get(0), builtReaders.get(2));
        assertNotSame(builtReaders.get(1), builtReaders.get(2));
        assertThat(readers.readReaders(), containsInAnyOrder(builtReaders.toArray()));
        assertThat(
            builtReaders.stream().map(TrackingFormatReader::readPath).toList(),
            containsInAnyOrder(entries.stream().map(StorageEntry::path).toArray())
        );
        assertAllBuiltReadersClosed(readers);
    }

    public void testDeferredExtractionKeepsTheBuiltReaderOpenUntilTheExtractorRegistryCloses() throws IOException {
        TrackingFormatReaderFactory readers = new TrackingFormatReaderFactory();
        List<Attribute> attributes = List.of(field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));
        AsyncExternalSourceOperatorFactory factory = executionFactory(readers, Runnable::run, true, attributes);

        DriverContext driverContext = driverContext();
        SourceExtractors registry = factory.sourceExtractorsFor(driverContext);
        drainToCompletion(factory, driverContext);

        TrackingFormatReader reader = onlyBuiltReader(readers);
        assertSame(reader, onlyReadReader(readers));
        assertEquals(1, registry.size());
        assertFalse("the registered extractor still owns the runtime reader", reader.isClosed());

        registry.close();

        assertTrue(reader.isClosed());
        assertAllBuiltReadersClosed(readers);
    }

    private static void assertNoReadersBuilt(TrackingFormatReaderFactory readers) {
        assertEquals(List.of(), readers.builtReaders());
        assertEquals(List.of(), readers.closedReaders());
    }

    private static void assertAllBuiltReadersClosed(TrackingFormatReaderFactory readers) {
        assertThat(readers.closedReaders(), containsInAnyOrder(readers.builtReaders().toArray()));
        for (TrackingFormatReader reader : readers.builtReaders()) {
            assertTrue(reader.isClosed());
        }
    }

    private static TrackingFormatReader onlyBuiltReader(TrackingFormatReaderFactory readers) {
        assertEquals(1, readers.builtReaders().size());
        return readers.builtReaders().get(0);
    }

    private static TrackingFormatReader onlyMetadataReader(TrackingFormatReaderFactory readers) {
        assertEquals(1, readers.metadataReaders().size());
        return readers.metadataReaders().get(0);
    }

    private static TrackingFormatReader onlyReadReader(TrackingFormatReaderFactory readers) {
        assertEquals(1, readers.readReaders().size());
        return readers.readReaders().get(0);
    }

    private static FileSourceFactory newFileSourceFactory(TrackingFormatReaderFactory readers) {
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("test-format", readers, Settings.EMPTY, BLOCK_FACTORY);
        formatRegistry.registerExtension(".test", "test-format");

        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(StubStorageProvider::new));

        return new FileSourceFactory(storageRegistry, formatRegistry, new DecompressionCodecRegistry(), Settings.EMPTY);
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(TrackingFormatReaderFactory readers) {
        return executionFactory(readers, Runnable::run);
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(TrackingFormatReaderFactory readers, Executor executor) {
        return executionFactory(readers, executor, false, List.of());
    }

    private static AsyncExternalSourceOperatorFactory executionFactory(
        TrackingFormatReaderFactory readers,
        Executor executor,
        boolean deferredExtraction,
        List<Attribute> attributes
    ) {
        StoragePath path = StoragePath.of("s3://bucket/data.test");
        SourceOperator.SourceOperatorFactory built = newFileSourceFactory(readers).operatorFactory()
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
        return field(name, DataType.INTEGER);
    }

    private static Attribute field(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

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

    private static final class TrackingFormatReaderFactory implements FormatReaderFactory {
        private final TrackingState state;

        TrackingFormatReaderFactory() {
            this(null, null);
        }

        private TrackingFormatReaderFactory(IOException metadataFailure, IOException readFailure) {
            this.state = new TrackingState(metadataFailure, readFailure);
        }

        static TrackingFormatReaderFactory failingMetadata(IOException failure) {
            return new TrackingFormatReaderFactory(failure, null);
        }

        static TrackingFormatReaderFactory failingRead(IOException failure) {
            return new TrackingFormatReaderFactory(null, failure);
        }

        @Override
        public FormatReader create(Settings settings, BlockFactory blockFactory) {
            return create(settings, blockFactory, null, FormatReadContext.Binding.empty());
        }

        @Override
        public FormatReader create(
            Settings settings,
            BlockFactory blockFactory,
            Map<String, Object> config,
            FormatReadContext.Binding binding
        ) {
            Map<String, Object> immutableConfig = config == null ? Map.of() : Map.copyOf(config);
            List<Attribute> schema = binding == null || binding.boundSchema() == null ? List.of() : List.copyOf(binding.boundSchema());
            String readConfig = binding == null ? null : binding.readConfig();
            TrackingFormatReader reader = new TrackingFormatReader(state, immutableConfig, schema, readConfig);
            state.builtReaders.add(reader);
            return reader;
        }

        @Override
        public Configured<Void> inspect(Map<String, Object> config) {
            state.inspectCount.incrementAndGet();
            Map<String, Object> immutableConfig = config == null ? Map.of() : Map.copyOf(config);
            state.configurations.add(immutableConfig);
            return Configured.fromKnownSubset(null, config, KNOWN_CONFIG_KEYS);
        }

        @Override
        public String formatName() {
            return "test-format";
        }

        @Override
        public boolean columnExtractor() {
            return true;
        }

        int inspectCount() {
            return state.inspectCount.get();
        }

        List<Map<String, Object>> configurations() {
            return List.copyOf(state.configurations);
        }

        List<TrackingFormatReader> builtReaders() {
            return List.copyOf(state.builtReaders);
        }

        List<TrackingFormatReader> metadataReaders() {
            return List.copyOf(state.metadataReaders);
        }

        List<TrackingFormatReader> readReaders() {
            return List.copyOf(state.readReaders);
        }

        List<TrackingFormatReader> closedReaders() {
            return List.copyOf(state.closedReaders);
        }
    }

    private static final class TrackingState {
        private final IOException metadataFailure;
        private final IOException readFailure;
        private final AtomicInteger inspectCount = new AtomicInteger();
        private final List<Map<String, Object>> configurations = new CopyOnWriteArrayList<>();
        private final List<TrackingFormatReader> builtReaders = new CopyOnWriteArrayList<>();
        private final List<TrackingFormatReader> metadataReaders = new CopyOnWriteArrayList<>();
        private final List<TrackingFormatReader> readReaders = new CopyOnWriteArrayList<>();
        private final List<TrackingFormatReader> closedReaders = new CopyOnWriteArrayList<>();

        TrackingState(IOException metadataFailure, IOException readFailure) {
            this.metadataFailure = metadataFailure;
            this.readFailure = readFailure;
        }
    }

    private static final Set<String> KNOWN_CONFIG_KEYS = Set.of("delimiter");

    private static final class TrackingFormatReader implements FormatReader {
        private final TrackingState state;
        private final Map<String, Object> config;
        private final List<Attribute> schema;
        private final String readConfig;
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile boolean openDuringMetadata;
        private volatile FormatReadContext readContext;
        private volatile StoragePath readPath;

        TrackingFormatReader(TrackingState state, Map<String, Object> config, List<Attribute> schema, String readConfig) {
            this.state = state;
            this.config = config;
            this.schema = schema;
            this.readConfig = readConfig;
        }

        Map<String, Object> config() {
            return config;
        }

        List<Attribute> schema() {
            return schema;
        }

        String readConfig() {
            return readConfig;
        }

        boolean wasOpenDuringMetadata() {
            return openDuringMetadata;
        }

        FormatReadContext readContext() {
            return readContext;
        }

        StoragePath readPath() {
            return readPath;
        }

        boolean isClosed() {
            return closed.get();
        }

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            state.metadataReaders.add(this);
            openDuringMetadata = closed.get() == false;
            if (state.metadataFailure != null) {
                throw state.metadataFailure;
            }
            return new SimpleSourceMetadata(List.of(), "file", object.path().toString());
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            state.readReaders.add(this);
            readContext = context;
            readPath = object.path();
            if (state.readFailure != null) {
                throw state.readFailure;
            }
            return new TrackingPageIterator(context.projectedColumns());
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                state.closedReaders.add(this);
            }
        }

        @Override
        public String toString() {
            return "TrackingFormatReader[config=" + config + ", schema=" + schema + ", readConfig=" + readConfig + "]";
        }
    }

    private static final class TrackingPageIterator implements CloseableIterator<Page>, ColumnExtractorProducer {
        private final List<String> projectedColumns;
        private boolean consumed;
        private long extractorHighBits = -1L;

        TrackingPageIterator(List<String> projectedColumns) {
            this.projectedColumns = projectedColumns;
        }

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
            Block[] blocks = new Block[projectedColumns.size()];
            for (int i = 0; i < projectedColumns.size(); i++) {
                if (ColumnExtractor.ROW_POSITION_COLUMN.equals(projectedColumns.get(i))) {
                    if (extractorHighBits < 0) {
                        throw new IllegalStateException("extractor id was not installed before reading row positions");
                    }
                    blocks[i] = BLOCK_FACTORY.newLongArrayVector(new long[] { extractorHighBits }, 1).asBlock();
                } else {
                    blocks[i] = BLOCK_FACTORY.newConstantNullBlock(1);
                }
            }
            return new Page(1, blocks);
        }

        @Override
        public ColumnExtractor createColumnExtractor(Consumer<String> driverThreadWarningSink) {
            return new TrackingColumnExtractor();
        }

        @Override
        public void setExtractorId(int id) {
            extractorHighBits = ((long) id) << ColumnExtractor.LOCAL_POSITION_BITS;
        }

        @Override
        public void close() {}
    }

    private static final class TrackingColumnExtractor implements ColumnExtractor {
        @Override
        public long rowCount() {
            return 1;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] positions, BlockFactory blockFactory) {
            throw new UnsupportedOperationException("the lifecycle test does not materialize deferred columns");
        }

        @Override
        public void close() {}
    }

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
