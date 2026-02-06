/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.apache.lucene.index.LeafReaderContext;
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
    public AllReader reader(LeafReaderContext context) throws IOException {
        AllReader encodedDigestReader = encodedDigestLoader.reader(context);
        AllReader minimaReader = minimaLoader.reader(context);
        AllReader maximaReader = maximaLoader.reader(context);
        AllReader sumsReader = sumsLoader.reader(context);
        AllReader valueCountsReader = valueCountsLoader.reader(context);

        return new TDigestReader(encodedDigestReader, minimaReader, maximaReader, sumsReader, valueCountsReader);
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.tdigestBlockBuilder(expectedCount);
    }

    static class TDigestReader implements AllReader {

        private final AllReader encodedDigestReader;
        private final AllReader minimaReader;
        private final AllReader maximaReader;
        private final AllReader sumsReader;
        private final AllReader valueCountsReader;

        TDigestReader(
            AllReader encodedDigestReader,
            AllReader minimaReader,
            AllReader maximaReader,
            AllReader sumsReader,
            AllReader valueCountsReader
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
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            TDigestBuilder histogramBuilder = (TDigestBuilder) builder;
            minimaReader.read(docId, storedFields, histogramBuilder.minima());
            maximaReader.read(docId, storedFields, histogramBuilder.maxima());
            sumsReader.read(docId, storedFields, histogramBuilder.sums());
            valueCountsReader.read(docId, storedFields, histogramBuilder.valueCounts());
            encodedDigestReader.read(docId, storedFields, histogramBuilder.encodedDigests());
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.TDigest";
        }
    }
}
