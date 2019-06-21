/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetReader.DocumentSubsetDirectoryReader;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * An {@link IndexSearcherWrapper} implementation that is used for field and document level security.
 * <p>
 * Based on the {@link ThreadContext} this class will enable field and/or document level security.
 * <p>
 * Field level security is enabled by wrapping the original {@link DirectoryReader} in a {@link FieldSubsetReader}
 * in the {@link #wrap(DirectoryReader)} method.
 * <p>
 * Document level security is enabled by wrapping the original {@link DirectoryReader} in a {@link DocumentSubsetReader}
 * instance.
 */
public class SecurityIndexSearcherWrapper extends IndexSearcherWrapper {
    private static final Logger logger = LogManager.getLogger(SecurityIndexSearcherWrapper.class);

    private final Function<ShardId, QueryShardContext> queryShardContextProvider;
    private final BitsetFilterCache bitsetFilterCache;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final ScriptService scriptService;

    public SecurityIndexSearcherWrapper(Function<ShardId, QueryShardContext> queryShardContextProvider,
                                        BitsetFilterCache bitsetFilterCache, ThreadContext threadContext, XPackLicenseState licenseState,
                                        ScriptService scriptService) {
        this.scriptService = scriptService;
        this.queryShardContextProvider = queryShardContextProvider;
        this.bitsetFilterCache = bitsetFilterCache;
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    protected DirectoryReader wrap(final DirectoryReader reader) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed() == false) {
            return reader;
        }

        try {
            final IndicesAccessControl indicesAccessControl = getIndicesAccessControl();

            ShardId shardId = ShardUtils.extractShardId(reader);
            if (shardId == null) {
                throw new IllegalStateException(LoggerMessageFormat.format("couldn't extract shardId from reader [{}]", reader));
            }

            final IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
            // No permissions have been defined for an index, so don't intercept the index reader for access control
            if (permissions == null) {
                return reader;
            }

            DirectoryReader wrappedReader = reader;
            DocumentPermissions documentPermissions = permissions.getDocumentPermissions();
            if (documentPermissions != null && documentPermissions.hasDocumentLevelPermissions()) {
                BooleanQuery filterQuery = documentPermissions.filter(getUser(), scriptService, shardId, queryShardContextProvider);
                if (filterQuery != null) {
                    wrappedReader = DocumentSubsetReader.wrap(wrappedReader, bitsetFilterCache, new ConstantScoreQuery(filterQuery));
                }
            }

            return permissions.getFieldPermissions().filter(wrappedReader);
        } catch (IOException e) {
            logger.error("Unable to apply field level security");
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    protected IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed() == false) {
            return searcher;
        }

        final DirectoryReader directoryReader = (DirectoryReader) searcher.getIndexReader();
        if (directoryReader instanceof DocumentSubsetDirectoryReader) {
            // The reasons why we return a custom searcher:
            // 1) in the case the role query is sparse then large part of the main query can be skipped
            // 2) If the role query doesn't match with any docs in a segment, that a segment can be skipped
            IndexSearcher searcherWrapper = new IndexSearcherWrapper((DocumentSubsetDirectoryReader) directoryReader);
            searcherWrapper.setQueryCache(searcher.getQueryCache());
            searcherWrapper.setQueryCachingPolicy(searcher.getQueryCachingPolicy());
            searcherWrapper.setSimilarity(searcher.getSimilarity());
            return searcherWrapper;
        }
        return searcher;
    }

    static class IndexSearcherWrapper extends IndexSearcher {

        IndexSearcherWrapper(DocumentSubsetDirectoryReader r) {
            super(r);
        }

        @Override
        protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
            for (LeafReaderContext ctx : leaves) { // search each subreader
                final LeafCollector leafCollector;
                try {
                    leafCollector = collector.getLeafCollector(ctx);
                } catch (CollectionTerminatedException e) {
                    // there is no doc of interest in this reader context
                    // continue with the following leaf
                    continue;
                }
                // The reader is always of type DocumentSubsetReader when we get here:
                DocumentSubsetReader reader = (DocumentSubsetReader) ctx.reader();

                BitSet roleQueryBits = reader.getRoleQueryBits();
                if (roleQueryBits == null) {
                    // nothing matches with the role query, so skip this segment:
                    continue;
                }

                // if the role query result set is sparse then we should use the SparseFixedBitSet for advancing:
                if (roleQueryBits instanceof SparseFixedBitSet) {
                    Scorer scorer = weight.scorer(ctx);
                    if (scorer != null) {
                        SparseFixedBitSet sparseFixedBitSet = (SparseFixedBitSet) roleQueryBits;
                        Bits realLiveDocs = reader.getWrappedLiveDocs();
                        try {
                            intersectScorerAndRoleBits(scorer, sparseFixedBitSet, leafCollector, realLiveDocs);
                        } catch (CollectionTerminatedException e) {
                            // collection was terminated prematurely
                            // continue with the following leaf
                        }
                    }
                } else {
                    BulkScorer bulkScorer = weight.bulkScorer(ctx);
                    if (bulkScorer != null) {
                        Bits liveDocs = reader.getLiveDocs();
                        try {
                            bulkScorer.score(leafCollector, liveDocs);
                        } catch (CollectionTerminatedException e) {
                            // collection was terminated prematurely
                            // continue with the following leaf
                        }
                    }
                }
            }
        }
    }

    static void intersectScorerAndRoleBits(Scorer scorer, SparseFixedBitSet roleBits, LeafCollector collector, Bits acceptDocs) throws
            IOException {
        // ConjunctionDISI uses the DocIdSetIterator#cost() to order the iterators, so if roleBits has the lowest cardinality it should
        // be used first:
        DocIdSetIterator iterator = ConjunctionDISI.intersectIterators(Arrays.asList(new BitSetIterator(roleBits,
                roleBits.approximateCardinality()), scorer.iterator()));
        for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
            if (acceptDocs == null || acceptDocs.get(docId)) {
                collector.collect(docId);
            }
        }
    }

    protected IndicesAccessControl getIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            throw Exceptions.authorizationError("no indices permissions found");
        }
        return indicesAccessControl;
    }

    protected User getUser(){
        Authentication authentication = Authentication.getAuthentication(threadContext);
        return authentication.getUser();
    }

}
