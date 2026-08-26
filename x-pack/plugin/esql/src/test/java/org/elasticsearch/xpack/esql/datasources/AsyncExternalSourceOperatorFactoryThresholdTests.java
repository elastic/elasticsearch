/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThresholdAware;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncExternalSourceOperatorFactoryThresholdTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();
    private static final StoragePath PATH = StoragePath.of("s3://bucket/data.parquet");
    private static final List<Attribute> ATTRIBUTES = List.of(
        new FieldAttribute(Source.EMPTY, "value", new EsField("value", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
    );

    public void testCachedThresholdIsSharedAcrossConcurrentNativeReads() {
        ManualAsyncReader reader = new ManualAsyncReader();
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).build();
        installThreshold(factory, false);

        SourceOperator first = factory.get(driverContext());
        SourceOperator second = factory.get(driverContext());

        assertThat(reader.thresholds.get(1), sameInstance(reader.thresholds.get(0)));

        reader.completeAll();
        first.close();
        second.close();
    }

    public void testSetNumericThresholdSupplierAfterOperatorCreationThrows() {
        ManualAsyncReader reader = new ManualAsyncReader();
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).build();

        SourceOperator operator = factory.get(driverContext());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> installThreshold(factory, false));

        assertThat(e.getMessage(), containsString("numeric threshold must be installed before source operators are created"));
        reader.completeAll();
        operator.close();
    }

    public void testNoFurtherCandidatesShortCircuitsSyncWrapperRead() {
        CountingReader reader = new CountingReader(false);
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).build();
        SharedNumericThreshold threshold = installExhaustedThreshold(factory);

        SourceOperator operator = factory.get(driverContext());

        assertEquals(0, reader.reads.get());
        assertTrue(operator.isFinished());
        operator.close();
        threshold.close();
    }

    public void testNoFurtherCandidatesShortCircuitsNativeAsyncRead() {
        ManualAsyncReader reader = new ManualAsyncReader();
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).build();
        SharedNumericThreshold threshold = installExhaustedThreshold(factory);

        SourceOperator operator = factory.get(driverContext());

        assertEquals(0, reader.asyncReads.get());
        assertTrue(operator.isFinished());
        operator.close();
        threshold.close();
    }

    public void testNoFurtherCandidatesShortCircuitsMultiFileProducer() {
        CountingReader reader = new CountingReader(false);
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 100, Instant.EPOCH)
        );
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).fileList(
            GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet")
        ).build();
        SharedNumericThreshold threshold = installExhaustedThreshold(factory);

        SourceOperator operator = factory.get(driverContext());

        assertEquals(0, reader.reads.get());
        assertTrue(operator.isFinished());
        operator.close();
        threshold.close();
    }

    public void testNoFurtherCandidatesShortCircuitsSliceQueueProducer() {
        CountingReader reader = new CountingReader(false);
        FileSplit split = new FileSplit("test", PATH, 0, 100, "parquet", Map.of(FileSplitProvider.LAST_SPLIT_KEY, "true"), Map.of());
        AsyncExternalSourceOperatorFactory factory = baseFactory(reader).sliceQueue(new ExternalSliceQueue(List.of(split))).build();
        SharedNumericThreshold threshold = installExhaustedThreshold(factory);

        SourceOperator operator = factory.get(driverContext());

        assertEquals(0, reader.reads.get());
        assertTrue(operator.isFinished());
        operator.close();
        threshold.close();
    }

    private static AsyncExternalSourceOperatorFactory.Builder baseFactory(FormatReader reader) {
        return AsyncExternalSourceOperatorFactory.builder(new TestStorageProvider(), reader, PATH, ATTRIBUTES, 100, 10, Runnable::run);
    }

    private static void installThreshold(AsyncExternalSourceOperatorFactory factory, boolean nullsFirst) {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(true, nullsFirst);
        factory.setNumericThresholdSupplier(supplier, "value", ElementType.LONG, true, nullsFirst);
    }

    private static SharedNumericThreshold installExhaustedThreshold(AsyncExternalSourceOperatorFactory factory) {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(true, true);
        SharedNumericThreshold threshold = supplier.get();
        threshold.markNoFurtherCandidates();
        factory.setNumericThresholdSupplier(supplier, "value", ElementType.LONG, true, true);
        return threshold;
    }

    private static DriverContext driverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        return driverContext;
    }

    private static CloseableIterator<Page> emptyIterator() {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    private static class CountingReader implements NoConfigFormatReader, DynamicThresholdAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger reads;
        private final DynamicThreshold threshold;

        CountingReader(boolean ignored) {
            this(new AtomicInteger(), null);
        }

        private CountingReader(AtomicInteger reads, DynamicThreshold threshold) {
            this.reads = reads;
            this.threshold = threshold;
        }

        @Override
        public FormatReader withDynamicThreshold(DynamicThreshold threshold) {
            return new CountingReader(reads, threshold);
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            assertNotNull(threshold);
            reads.incrementAndGet();
            return emptyIterator();
        }

        @Override
        public String formatName() {
            return "counting";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class ManualAsyncReader implements NoConfigFormatReader, DynamicThresholdAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger asyncReads;
        private final List<DynamicThreshold> thresholds;
        private final List<ActionListener<CloseableIterator<Page>>> listeners;
        private final DynamicThreshold threshold;

        ManualAsyncReader() {
            this(new AtomicInteger(), new ArrayList<>(), new ArrayList<>(), null);
        }

        private ManualAsyncReader(
            AtomicInteger asyncReads,
            List<DynamicThreshold> thresholds,
            List<ActionListener<CloseableIterator<Page>>> listeners,
            DynamicThreshold threshold
        ) {
            this.asyncReads = asyncReads;
            this.thresholds = thresholds;
            this.listeners = listeners;
            this.threshold = threshold;
        }

        @Override
        public FormatReader withDynamicThreshold(DynamicThreshold threshold) {
            thresholds.add(threshold);
            return new ManualAsyncReader(asyncReads, thresholds, listeners, threshold);
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readAsync(
            StorageObject object,
            FormatReadContext context,
            java.util.concurrent.Executor executor,
            ActionListener<CloseableIterator<Page>> listener
        ) {
            asyncReads.incrementAndGet();
            listeners.add(listener);
        }

        void completeAll() {
            List<ActionListener<CloseableIterator<Page>>> pending = List.copyOf(listeners);
            listeners.clear();
            for (ActionListener<CloseableIterator<Page>> listener : pending) {
                listener.onResponse(emptyIterator());
            }
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return emptyIterator();
        }

        @Override
        public String formatName() {
            return "manual-async";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class TestStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new TestStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new TestStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new TestStorageObject(path);
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

    private static class TestStorageObject implements StorageObject {
        private final StoragePath path;

        TestStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(new byte[0]);
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
