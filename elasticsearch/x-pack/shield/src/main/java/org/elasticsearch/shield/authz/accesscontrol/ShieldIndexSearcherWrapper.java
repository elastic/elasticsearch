/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.shield.authz.accesscontrol.DocumentSubsetReader.DocumentSubsetDirectoryReader;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.shield.support.Exceptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

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
public class ShieldIndexSearcherWrapper extends IndexSearcherWrapper {

    private final MapperService mapperService;
    private final Set<String> allowedMetaFields;
    private final QueryShardContext queryShardContext;
    private final BitsetFilterCache bitsetFilterCache;
    private final SecurityLicenseState shieldLicenseState;
    private final ThreadContext threadContext;
    private final ESLogger logger;

    public ShieldIndexSearcherWrapper(IndexSettings indexSettings, QueryShardContext queryShardContext,
                                      MapperService mapperService, BitsetFilterCache bitsetFilterCache,
                                      ThreadContext threadContext, SecurityLicenseState shieldLicenseState) {
        this.logger = Loggers.getLogger(getClass(), indexSettings.getSettings());
        this.mapperService = mapperService;
        this.queryShardContext = queryShardContext;
        this.bitsetFilterCache = bitsetFilterCache;
        this.threadContext = threadContext;
        this.shieldLicenseState = shieldLicenseState;

        Set<String> allowedMetaFields = new HashSet<>();
        allowedMetaFields.addAll(Arrays.asList(MapperService.getAllMetaFields()));
        allowedMetaFields.add("_source"); // TODO: add _source to MapperService#META_FIELDS?
        allowedMetaFields.add("_version"); // TODO: add _version to MapperService#META_FIELDS?
        allowedMetaFields.remove("_all"); // The _all field contains actual data and we can't include that by default.

        this.allowedMetaFields = Collections.unmodifiableSet(allowedMetaFields);
    }

    @Override
    protected DirectoryReader wrap(DirectoryReader reader) {
        if (shieldLicenseState.documentAndFieldLevelSecurityEnabled() == false) {
            return reader;
        }

        final Set<String> allowedMetaFields = this.allowedMetaFields;
        try {
            final IndicesAccessControl indicesAccessControl = getIndicesAccessControl();

            ShardId shardId = ShardUtils.extractShardId(reader);
            if (shardId == null) {
                throw new IllegalStateException(LoggerMessageFormat.format("couldn't extract shardId from reader [{}]", reader));
            }

            IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
            // No permissions have been defined for an index, so don't intercept the index reader for access control
            if (permissions == null) {
                return reader;
            }

            if (permissions.getQueries() != null) {
                BooleanQuery.Builder filter = new BooleanQuery.Builder();
                for (BytesReference bytesReference : permissions.getQueries()) {
                    QueryShardContext queryShardContext = copyQueryShardContext(this.queryShardContext);
                    ParsedQuery parsedQuery = queryShardContext.parse(bytesReference);
                    filter.add(parsedQuery.query(), SHOULD);
                }
                // at least one of the queries should match
                filter.setMinimumNumberShouldMatch(1);
                reader = DocumentSubsetReader.wrap(reader, bitsetFilterCache, new ConstantScoreQuery(filter.build()));
            }

            if (permissions.getFields() != null) {
                // now add the allowed fields based on the current granted permissions and :
                Set<String> allowedFields = new HashSet<>(allowedMetaFields);
                for (String field : permissions.getFields()) {
                    allowedFields.addAll(mapperService.simpleMatchToIndexNames(field));
                }
                resolveParentChildJoinFields(allowedFields);
                reader = FieldSubsetReader.wrap(reader, allowedFields);
            }

            return reader;
        } catch (IOException e) {
            logger.error("Unable to apply field level security");
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    protected IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
        if (shieldLicenseState.documentAndFieldLevelSecurityEnabled() == false) {
            return searcher;
        }

        final DirectoryReader directoryReader = (DirectoryReader) searcher.getIndexReader();
        if (directoryReader instanceof DocumentSubsetDirectoryReader) {
            // The reasons why we return a custom searcher:
            // 1) in the case the role query is sparse then large part of the main query can be skipped
            // 2) If the role query doesn't match with any docs in a segment, that a segment can be skipped
            IndexSearcher indexSearcher = new IndexSearcherWrapper((DocumentSubsetDirectoryReader) directoryReader);
            indexSearcher.setQueryCache(indexSearcher.getQueryCache());
            indexSearcher.setQueryCachingPolicy(indexSearcher.getQueryCachingPolicy());
            indexSearcher.setSimilarity(indexSearcher.getSimilarity(true));
            return indexSearcher;
        }
        return searcher;
    }

    static class IndexSearcherWrapper extends IndexSearcher {

        public IndexSearcherWrapper(DocumentSubsetDirectoryReader r) {
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

    public Set<String> getAllowedMetaFields() {
        return allowedMetaFields;
    }

    // for testing:
    protected QueryShardContext copyQueryShardContext(QueryShardContext context) {
        return new QueryShardContext(context);
    }

    private void resolveParentChildJoinFields(Set<String> allowedFields) {
        for (DocumentMapper mapper : mapperService.docMappers(false)) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                String joinField = ParentFieldMapper.joinField(parentFieldMapper.type());
                allowedFields.add(joinField);
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
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            throw Exceptions.authorizationError("no indices permissions found");
        }
        return indicesAccessControl;
    }
}
