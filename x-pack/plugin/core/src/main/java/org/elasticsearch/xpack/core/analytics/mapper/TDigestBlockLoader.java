/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;

import java.io.IOException;

public class TDigestBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final DoublesBlockLoader minimaLoader;
    private final DoublesBlockLoader maximaLoader;
    private final DoublesBlockLoader sumsLoader;
    private final LongsBlockLoader valueCountsLoader;
    private final BytesRefsFromBinaryBlockLoader encodedDigestLoader;

    public TDigestBlockLoader(
        BytesRefsFromBinaryBlockLoader encodedDigestLoader,
        DoublesBlockLoader minimaLoader,
        DoublesBlockLoader maximaLoader,
        DoublesBlockLoader sumsLoader,
        LongsBlockLoader valueCountsLoader
    ) {
        this.encodedDigestLoader = encodedDigestLoader;
        this.minimaLoader = minimaLoader;
        this.maximaLoader = maximaLoader;
        this.sumsLoader = sumsLoader;
        this.valueCountsLoader = valueCountsLoader;
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        ColumnAtATimeReader encodedDigestReader = null;
        ColumnAtATimeReader minimaReader = null;
        ColumnAtATimeReader maximaReader = null;
        ColumnAtATimeReader sumsReader = null;
        ColumnAtATimeReader valueCountsReader = null;
        try {
            encodedDigestReader = encodedDigestLoader.reader(breaker, context);
            minimaReader = minimaLoader.reader(breaker, context);
            maximaReader = maximaLoader.reader(breaker, context);
            sumsReader = sumsLoader.reader(breaker, context);
            valueCountsReader = valueCountsLoader.reader(breaker, context);
        } finally {
            if (valueCountsReader == null) {
                Releasables.close(encodedDigestReader, minimaReader, maximaReader, sumsReader, valueCountsReader);
            }
        }

        return new TDigestReader(encodedDigestReader, minimaReader, maximaReader, sumsReader, valueCountsReader);
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.tdigestBlockBuilder(expectedCount);
    }

    static class TDigestReader implements ColumnAtATimeReader {

        private final ColumnAtATimeReader encodedDigestReader;
        private final ColumnAtATimeReader minimaReader;
        private final ColumnAtATimeReader maximaReader;
        private final ColumnAtATimeReader sumsReader;
        private final ColumnAtATimeReader valueCountsReader;

        TDigestReader(
            ColumnAtATimeReader encodedDigestReader,
            ColumnAtATimeReader minimaReader,
            ColumnAtATimeReader maximaReader,
            ColumnAtATimeReader sumsReader,
            ColumnAtATimeReader valueCountsReader
        ) {
            this.encodedDigestReader = encodedDigestReader;
            this.minimaReader = minimaReader;
            this.maximaReader = maximaReader;
            this.sumsReader = sumsReader;
            this.valueCountsReader = valueCountsReader;
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return minimaReader.canReuse(startingDocID)
                && maximaReader.canReuse(startingDocID)
                && sumsReader.canReuse(startingDocID)
                && valueCountsReader.canReuse(startingDocID)
                && encodedDigestReader.canReuse(startingDocID);
        }

        @Override
        // Column oriented reader
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            Block minima = null;
            Block maxima = null;
            Block sums = null;
            Block valueCounts = null;
            Block encodedBytes = null;
            Block result;
            boolean success = false;
            try {
                minima = minimaReader.read(factory, docs, offset, nullsFiltered);
                maxima = maximaReader.read(factory, docs, offset, nullsFiltered);
                sums = sumsReader.read(factory, docs, offset, nullsFiltered);
                valueCounts = valueCountsReader.read(factory, docs, offset, nullsFiltered);
                encodedBytes = encodedDigestReader.read(factory, docs, offset, nullsFiltered);
                result = factory.buildTDigestBlockDirect(encodedBytes, minima, maxima, sums, valueCounts);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(minima, maxima, sums, valueCounts, encodedBytes);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.TDigest";
        }

        @Override
        public void close() {
            Releasables.close(encodedDigestReader, minimaReader, maximaReader, sumsReader, valueCountsReader);
        }
    }
}
