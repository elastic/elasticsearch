/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Test factory whose {@link #create} returns a distinct owned wrapper every time.
 */
class TestFormatReaderFactory implements FormatReaderFactory {

    final Supplier<? extends FormatReader> readerSupplier;
    private final String formatName;
    private final ErrorPolicy defaultErrorPolicy;
    private final AggregatePushdownSupport aggregatePushdownSupport;
    private final boolean supportsNativeAsync;
    private final boolean columnExtractor;
    private final boolean headerRow;
    private final boolean dropsRowsUnderPushedFilter;

    static FormatReaderFactory of(FormatReader reader) {
        return new TestFormatReaderFactory(reader);
    }

    static TestFormatReaderFactory basic(Supplier<? extends FormatReader> readerSupplier) {
        return new TestFormatReaderFactory(
            readerSupplier,
            "test",
            ErrorPolicy.STRICT,
            AggregatePushdownSupport.UNSUPPORTED,
            false,
            false,
            false,
            false
        );
    }

    static FormatReaderFactory columnExtracting(Supplier<? extends FormatReader> readerSupplier) {
        return new TestFormatReaderFactory(
            readerSupplier,
            "test",
            ErrorPolicy.STRICT,
            AggregatePushdownSupport.UNSUPPORTED,
            false,
            true,
            false,
            false
        );
    }

    TestFormatReaderFactory(FormatReader reader) {
        this(() -> reader, "test", ErrorPolicy.STRICT, AggregatePushdownSupport.UNSUPPORTED, false, false, false, false);
    }

    private TestFormatReaderFactory(
        Supplier<? extends FormatReader> readerSupplier,
        String formatName,
        ErrorPolicy defaultErrorPolicy,
        AggregatePushdownSupport aggregatePushdownSupport,
        boolean supportsNativeAsync,
        boolean columnExtractor,
        boolean headerRow,
        boolean dropsRowsUnderPushedFilter
    ) {
        this.readerSupplier = readerSupplier;
        this.formatName = formatName;
        this.defaultErrorPolicy = defaultErrorPolicy;
        this.aggregatePushdownSupport = aggregatePushdownSupport;
        this.supportsNativeAsync = supportsNativeAsync;
        this.columnExtractor = columnExtractor;
        this.headerRow = headerRow;
        this.dropsRowsUnderPushedFilter = dropsRowsUnderPushedFilter;
    }

    TestFormatReaderFactory withFormatName(String formatName) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withDefaultErrorPolicy(ErrorPolicy defaultErrorPolicy) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withAggregatePushdownSupport(AggregatePushdownSupport aggregatePushdownSupport) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withNativeAsync(boolean supportsNativeAsync) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withColumnExtractor(boolean columnExtractor) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withHeaderRow(boolean headerRow) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    TestFormatReaderFactory withDropsRowsUnderPushedFilter(boolean dropsRowsUnderPushedFilter) {
        return new TestFormatReaderFactory(
            readerSupplier,
            formatName,
            defaultErrorPolicy,
            aggregatePushdownSupport,
            supportsNativeAsync,
            columnExtractor,
            headerRow,
            dropsRowsUnderPushedFilter
        );
    }

    @Override
    public FormatReader create(Settings settings, BlockFactory blockFactory) {
        return wrap(readerSupplier.get());
    }

    @Override
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        return wrap(readerSupplier.get());
    }

    @Override
    public String formatName() {
        return formatName;
    }

    @Override
    public ErrorPolicy defaultErrorPolicy() {
        return defaultErrorPolicy;
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return aggregatePushdownSupport;
    }

    @Override
    public boolean dropsRowsUnderPushedFilter() {
        return dropsRowsUnderPushedFilter;
    }

    @Override
    public boolean supportsNativeAsync() {
        return supportsNativeAsync;
    }

    @Override
    public boolean rangeAware() {
        return readerSupplier.get() instanceof RangeAwareFormatReader;
    }

    @Override
    public boolean supportsBatchRead() {
        FormatReader reader = readerSupplier.get();
        return reader instanceof RangeAwareFormatReader rangeAware && rangeAware.supportsBatchRead();
    }

    @Override
    public boolean segmentable() {
        return readerSupplier.get() instanceof SegmentableFormatReader;
    }

    @Override
    public boolean columnExtractor() {
        return columnExtractor;
    }

    @Override
    public boolean headerRow(@Nullable Map<String, Object> config) {
        return headerRow;
    }

    @Override
    public RecordSplitter recordSplitter(@Nullable Map<String, Object> config, int maxRecordBytes) {
        FormatReader reader = readerSupplier.get();
        return reader instanceof SegmentableFormatReader segmentable ? segmentable.recordSplitter(maxRecordBytes) : null;
    }

    @Override
    public long minimumSegmentSize(@Nullable Map<String, Object> config) {
        FormatReader reader = readerSupplier.get();
        return reader instanceof SegmentableFormatReader segmentable ? segmentable.minimumSegmentSize() : 0L;
    }

    private static FormatReader wrap(FormatReader delegate) {
        if (delegate instanceof SegmentableFormatReader segmentable) {
            return new ForwardingSegmentableFormatReader(segmentable);
        }
        if (delegate instanceof RangeAwareFormatReader rangeAware) {
            return new ForwardingRangeAwareFormatReader(rangeAware);
        }
        return new ForwardingFormatReader(delegate);
    }

    private static class ForwardingFormatReader implements FormatReader {
        private final FormatReader delegate;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ForwardingFormatReader(FormatReader delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        protected final FormatReader delegate() {
            return delegate;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            return delegate.metadata(object);
        }

        @Override
        public void metadataAsync(StorageObject object, Executor executor, ActionListener<SourceMetadata> listener) {
            delegate.metadataAsync(object, executor, listener);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            return delegate.read(object, context);
        }

        @Override
        public void readAsync(
            StorageObject object,
            FormatReadContext context,
            Executor executor,
            ActionListener<CloseableIterator<Page>> listener
        ) {
            delegate.readAsync(object, context, executor, listener);
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return delegate.rowPositionStrategy();
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                delegate.close();
            }
        }
    }

    private static final class ForwardingRangeAwareFormatReader extends ForwardingFormatReader implements RangeAwareFormatReader {
        private final RangeAwareFormatReader delegate;

        private ForwardingRangeAwareFormatReader(RangeAwareFormatReader reader) {
            super(reader);
            this.delegate = reader;
        }

        @Override
        public List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException {
            return delegate.discoverSplitRanges(object);
        }

        @Override
        public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException {
            return delegate.readRange(object, context);
        }

        @Override
        public boolean supportsBatchRead() {
            return delegate.supportsBatchRead();
        }

        @Override
        public CloseableIterator<Page> readAll(List<SplitRef> splits, List<String> projectedColumns, int batchSize) throws IOException {
            return delegate.readAll(splits, projectedColumns, batchSize);
        }
    }

    private static final class ForwardingSegmentableFormatReader extends ForwardingFormatReader implements SegmentableFormatReader {
        private final SegmentableFormatReader delegate;

        private ForwardingSegmentableFormatReader(SegmentableFormatReader reader) {
            super(reader);
            this.delegate = reader;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return delegate.recordSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return delegate.minimumSegmentSize();
        }

        @Override
        public void acceptReadCpuNanos(long nanos) {
            delegate.acceptReadCpuNanos(nanos);
        }
    }
}
