/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.translog.TranslogStats;

import java.util.function.Function;

/**
 * Read-only engine for indices whose segments are stubs that reference Parquet files. A stub has no postings, so the lookup of an
 * {@code _id} that every engine does in {@code get} cannot work. The {@code _id} of a document is its row number in the index, and
 * because the stubs are attached in row order and never merge, the row number is the docID in the index reader.
 */
final class ParquetReadOnlyEngine extends ReadOnlyEngine {

    ParquetReadOnlyEngine(EngineConfig config) {
        super(config, null, new TranslogStats(), true, Function.identity(), false, true);
    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        SplitShardCountSummary splitShardCountSummary,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        Searcher searcher = acquireSearcher("get", SearcherScope.EXTERNAL, splitShardCountSummary, searcherWrapper);
        boolean found = false;
        try {
            DocIdAndVersion docIdAndVersion = resolve(searcher.getIndexReader(), get.id());
            if (docIdAndVersion == null) {
                return GetResult.NOT_EXISTS;
            }
            found = true;
            return new GetResult(searcher, docIdAndVersion, false);
        } finally {
            if (found == false) {
                Releasables.close(searcher);
            }
        }
    }

    /** The document with the given {@code _id}, which is its row number in the index, or {@code null} if there is none. */
    @Nullable
    static DocIdAndVersion resolve(IndexReader reader, String id) {
        final long row;
        try {
            row = Long.parseLong(id);
        } catch (NumberFormatException e) {
            return null;
        }
        // "05" and "+5" are not the id of row 5
        if (row < 0 || row >= reader.maxDoc() || Long.toString(row).equals(id) == false) {
            return null;
        }
        LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex((int) row, reader.leaves()));
        // version, sequence number and primary term are synthesized, because the stub has no such doc values
        return new DocIdAndVersion((int) row - leaf.docBase, 1L, 0L, 1L, leaf.reader(), leaf.docBase);
    }
}
