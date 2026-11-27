/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An expanding read through each of the factory's read paths: the factory hands every read context the
 * {@link ExpandRead}, gives the reader the parent limit as its row limit (never the source limiter's remaining rows,
 * which count elements), and strips the parent ordinals from the pages it delivers.
 */
public class AsyncExternalSourceOperatorFactoryExpandReadTests extends ESTestCase {

    private static final BlockFactory BLOCKS = TestBlockFactory.getNonBreakingInstance();
    private static final ExpandRead EXPAND = new ExpandRead("items", List.of(new ExpandRead.Leaf("qty", DataType.LONG)));
    private static final int PARENT_LIMIT = 7;
    private static final List<Attribute> ATTRIBUTES = List.of(
        new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
    );

    /**
     * A page whose block count is not the scan's columns plus the leaves and the ordinals is refused, and released: a
     * reader that did not expand would otherwise lose its last real column as the ordinals.
     */
    public void testPageOfTheWrongWidthIsRefused() {
        Block first = BLOCKS.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock();
        Page unexpanded = new Page(first, BLOCKS.newLongArrayVector(new long[] { 3, 4 }, 2).asBlock());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> AsyncExternalSourceOperatorFactory.withoutParentOrdinals(unexpanded, 3)
        );
        assertThat(e.getMessage(), containsString("[2] blocks where [3] were expected"));
        assertTrue(first.isReleased());
    }

    public void testSyncWrapperRead() throws Exception {
        RecordingReader reader = new RecordingReader(false);
        assertExpandingRead(builder(reader).rowLimit(3), reader, 1);
    }

    public void testNativeAsyncRead() throws Exception {
        RecordingReader reader = new RecordingReader(true);
        assertExpandingRead(builder(reader).rowLimit(3), reader, 1);
    }

    public void testMultiFileRead() throws Exception {
        RecordingReader reader = new RecordingReader(false);
        FileList files = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://bucket/data/a.parquet"), 100, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/data/b.parquet"), 100, Instant.EPOCH)
            ),
            "s3://bucket/data/*.parquet"
        );
        assertExpandingRead(builder(reader).fileList(files), reader, 2);
    }

    public void testSliceQueueRead() throws Exception {
        RecordingReader reader = new RecordingReader(false);
        ExternalSliceQueue queue = new ExternalSliceQueue(
            List.of(
                new FileSplit("test", StoragePath.of("s3://bucket/a.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
                new FileSplit("test", StoragePath.of("s3://bucket/b.parquet"), 0, 100, "parquet", Map.of(), Map.of())
            )
        );
        assertExpandingRead(builder(reader).sliceQueue(queue), reader, 2);
    }

    public void testSliceQueueRangeRead() throws Exception {
        RecordingReader reader = new RecordingReader(false);
        Map<String, Object> range = Map.of(FileSplitProvider.RANGE_SPLIT_KEY, "true");
        ExternalSliceQueue queue = new ExternalSliceQueue(
            List.of(new FileSplit("test", StoragePath.of("s3://bucket/a.parquet"), 0, 100, "parquet", range, Map.of()))
        );
        assertExpandingRead(builder(reader).sliceQueue(queue), reader, 1);
        assertThat(reader.rangeReads, equalTo(1));
    }

    /** Under {@code skip_row} a drop above the reader can remove parents after it, so the reader is not cut. */
    public void testNonStrictReadIsNotCutAtTheReader() {
        AsyncExternalSourceOperatorFactory factory = builder(new RecordingReader(false)).errorPolicy(ErrorPolicy.LENIENT).build();
        assertThat(factory.sourceReaderRowLimit(), equalTo(FormatReader.NO_LIMIT));
    }

    /** A reader that does not know an expand read would return unexpanded rows, so it is refused by name. */
    public void testReaderWithoutExpandReadIsRefused() {
        RecordingReader reader = new RecordingReader(false) {
            @Override
            public boolean supportsExpandRead() {
                return false;
            }
        };
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder(reader).build());
        assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset is not supported for the [recording] format"));
    }

    private void assertExpandingRead(AsyncExternalSourceOperatorFactory.Builder builder, RecordingReader reader, int reads)
        throws Exception {
        AsyncExternalSourceOperatorFactory factory = builder.build();
        assertThat(factory.sourceReaderRowLimit(), equalTo(PARENT_LIMIT));
        List<Page> pages = drain(factory);
        try {
            assertThat(reader.expandReads, hasSize(reads));
            assertThat(reader.expandReads, everyItem(equalTo(EXPAND)));
            assertThat(reader.rowLimits, everyItem(equalTo(PARENT_LIMIT)));
            assertThat(reader.statsScopes, everyItem(equalTo(StripeColumnScope.NONE)));
            for (Page page : pages) {
                assertThat("the parent ordinals are removed", page.getBlockCount(), equalTo(2));
            }
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    private static AsyncExternalSourceOperatorFactory.Builder builder(FormatReader reader) {
        return AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            reader,
            StoragePath.of("s3://bucket/a.parquet"),
            ATTRIBUTES,
            100,
            10,
            Runnable::run
        ).expandRead(EXPAND, PARENT_LIMIT);
    }

    private static List<Page> drain(AsyncExternalSourceOperatorFactory factory) {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(mock(BlockFactory.class));
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        List<Page> pages = new ArrayList<>();
        try (SourceOperator operator = factory.get(driverContext)) {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
        }
        return pages;
    }

    /**
     * Records what each read was told, and returns one expanded page: a parent column, a leaf column and the parent
     * ordinals, two elements of parent {@code 0} and one of parent {@code 1}.
     */
    private static class RecordingReader implements NoConfigFormatReader, RangeAwareFormatReader {
        final List<ExpandRead> expandReads = new CopyOnWriteArrayList<>();
        final List<Integer> rowLimits = new CopyOnWriteArrayList<>();
        final List<StripeColumnScope> statsScopes = new CopyOnWriteArrayList<>();
        int rangeReads;
        private final boolean nativeAsync;

        RecordingReader(boolean nativeAsync) {
            this.nativeAsync = nativeAsync;
        }

        @Override
        public boolean supportsExpandRead() {
            return true;
        }

        @Override
        public boolean supportsNativeAsync() {
            return nativeAsync;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            expandReads.add(context.expandRead());
            rowLimits.add(context.rowLimit());
            statsScopes.add(context.statsColumnScope());
            return onePage();
        }

        @Override
        public List<SplitRange> discoverSplitRanges(StorageObject object) {
            return List.of(new SplitRange(0, 100, Map.of()));
        }

        @Override
        public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) {
            rangeReads++;
            expandReads.add(context.expandRead());
            rowLimits.add(context.rowLimit());
            statsScopes.add(StripeColumnScope.NONE);
            return onePage();
        }

        private static CloseableIterator<Page> onePage() {
            Block id = BLOCKS.newIntArrayVector(new int[] { 1, 1, 2 }, 3).asBlock();
            Block qty = BLOCKS.newLongArrayVector(new long[] { 10, 11, 20 }, 3).asBlock();
            Block ordinals = BLOCKS.newIntArrayVector(new int[] { 0, 0, 1 }, 3).asBlock();
            Page page = new Page(id, qty, ordinals);
            return new CloseableIterator<>() {
                private boolean consumed;

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
                    return page;
                }

                @Override
                public void close() {
                    if (consumed == false) {
                        page.releaseBlocks();
                    }
                }
            };
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public String formatName() {
            return "recording";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class StubStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
        }

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

    private static class StubStorageObject extends AbstractTestStorageObject {
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
            return 100;
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
