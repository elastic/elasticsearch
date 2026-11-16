/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class FormatReadersTests extends ESTestCase {

    public void testCloseDistinctFromSkipsOwner() {
        CountingReader owner = new CountingReader();
        FormatReaders.closeDistinctFrom(owner, owner);
        assertEquals(0, owner.closes.get());
    }

    public void testCloseDistinctFromClosesRemintOnce() {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        FormatReaders.closeDistinctFrom(remint, owner);
        assertEquals(1, remint.closes.get());
        assertEquals(0, owner.closes.get());
    }

    public void testCloseDistinctFromNoopsOnNull() {
        CountingReader owner = new CountingReader();
        FormatReaders.closeDistinctFrom(null, owner);
        assertEquals(0, owner.closes.get());
    }

    public void testCloseDistinctFromStopsAtOwnerThroughCompressionWrapper() {
        CountingReader owner = new CountingReader();
        FormatReader wrapper = new CompressionDelegatingFormatReader(owner, new GzipDecompressionCodec());
        FormatReaders.closeDistinctFrom(wrapper, owner);
        assertEquals(0, owner.closes.get());
    }

    public void testCloseDistinctFromClosesWrappedRemintAndStopsAtOwner() {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        FormatReader wrapper = new CompressionDelegatingFormatReader(remint, new GzipDecompressionCodec());
        FormatReaders.closeDistinctFrom(wrapper, owner);
        assertEquals(1, remint.closes.get());
        assertEquals(0, owner.closes.get());
    }

    public void testReleasingIteratorClosesPagesThenRemint() throws Exception {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        AtomicBoolean pagesClosed = new AtomicBoolean();
        CloseableIterator<Page> pages = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                pagesClosed.set(true);
            }
        };
        CloseableIterator<Page> wrapped = FormatReaders.releasing(pages, owner, remint);
        wrapped.close();
        assertTrue(pagesClosed.get());
        assertEquals(1, remint.closes.get());
        assertEquals(0, owner.closes.get());
        wrapped.close();
        assertEquals(1, remint.closes.get());
    }

    public void testReleasingIteratorForwardsReadiness() throws Exception {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        SubscribableListener<Void> ready = new SubscribableListener<>();
        CloseableIterator<Page> pages = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public SubscribableListener<Void> waitForReady() {
                return ready;
            }

            @Override
            public boolean isExhausted() {
                return false;
            }

            @Override
            public void close() {}
        };
        CloseableIterator<Page> wrapped = FormatReaders.releasing(pages, owner, remint);
        assertSame(ready, wrapped.waitForReady());
        assertFalse(wrapped.isExhausted());
        wrapped.close();
        assertEquals(1, remint.closes.get());
    }

    public void testReleasingNullPagesClosesRemintImmediately() {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        assertNull(FormatReaders.releasing(null, owner, remint));
        assertEquals(1, remint.closes.get());
        assertEquals(0, owner.closes.get());
    }

    public void testReleasingIteratorForwardsColumnExtractorProducer() throws Exception {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        StubExtractor extractor = new StubExtractor();
        AtomicInteger extractorId = new AtomicInteger(-1);
        CloseableIterator<Page> pages = new ProducerPages(extractor, extractorId);
        CloseableIterator<Page> wrapped = FormatReaders.releasing(pages, owner, remint);
        assertTrue(wrapped instanceof ColumnExtractorProducer);
        ColumnExtractorProducer producer = (ColumnExtractorProducer) wrapped;
        assertSame(extractor, producer.createColumnExtractor(null));
        producer.setExtractorId(7);
        assertEquals(7, extractorId.get());
        wrapped.close();
        assertEquals(1, remint.closes.get());
    }

    public void testReleasingIteratorProducerFailsLoudWhenDelegateIsNotProducer() throws Exception {
        CountingReader owner = new CountingReader();
        CountingReader remint = new CountingReader();
        CloseableIterator<Page> pages = new CloseableIterator<>() {
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
        CloseableIterator<Page> wrapped = FormatReaders.releasing(pages, owner, remint);
        ColumnExtractorProducer producer = (ColumnExtractorProducer) wrapped;
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> producer.createColumnExtractor(null));
        assertTrue(e.getMessage().contains("ColumnExtractorProducer"));
        wrapped.close();
    }

    private static final class CountingReader implements NoConfigFormatReader {
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "csv";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static final class ProducerPages implements CloseableIterator<Page>, ColumnExtractorProducer {
        private final ColumnExtractor extractor;
        private final AtomicInteger extractorId;

        ProducerPages(ColumnExtractor extractor, AtomicInteger extractorId) {
            this.extractor = extractor;
            this.extractorId = extractorId;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Page next() {
            throw new NoSuchElementException();
        }

        @Override
        public ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) {
            return extractor;
        }

        @Override
        public void setExtractorId(int id) {
            extractorId.set(id);
        }

        @Override
        public void close() {}
    }

    private static final class StubExtractor implements ColumnExtractor {
        @Override
        public long rowCount() {
            return 0;
        }

        @Override
        public Block[] extract(String[] columnNames, @Nullable DataType[] targetTypes, long[] positions, BlockFactory blockFactory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
