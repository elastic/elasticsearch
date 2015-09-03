/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.IndexSearcherWrapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.shield.support.Exceptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

/**
 * An {@link IndexSearcherWrapper} implementation that is used for field and document level security.
 *
 * Based on the {@link RequestContext} this class will enable field and/or document level security.
 *
 * Field level security is enabled by wrapping the original {@link DirectoryReader} in a {@link FieldSubsetReader}
 * in the {@link #wrap(DirectoryReader)} method.
 *
 * Document level security is enabled by replacing the original {@link IndexSearcher} with a {@link ShieldIndexSearcherWrapper.ShieldIndexSearcher}
 * instance.
 */
public final class ShieldIndexSearcherWrapper extends AbstractIndexShardComponent implements IndexSearcherWrapper {

    private final MapperService mapperService;
    private final Set<String> allowedMetaFields;
    private final IndexQueryParserService parserService;

    private volatile boolean shardStarted = false;

    @Inject
    public ShieldIndexSearcherWrapper(ShardId shardId, @IndexSettings Settings indexSettings, IndexQueryParserService parserService, IndicesLifecycle indicesLifecycle, MapperService mapperService) {
        super(shardId, indexSettings);
        this.mapperService = mapperService;
        this.parserService = parserService;
        indicesLifecycle.addListener(new ShardLifecycleListener());

        Set<String> allowedMetaFields = new HashSet<>();
        allowedMetaFields.addAll(Arrays.asList(MapperService.getAllMetaFields()));
        allowedMetaFields.add("_source"); // TODO: add _source to MapperService#META_FIELDS?
        allowedMetaFields.add("_version"); // TODO: add _version to MapperService#META_FIELDS?
        allowedMetaFields.remove("_all"); // The _all field contains actual data and we can't include that by default.

        this.allowedMetaFields = Collections.unmodifiableSet(allowedMetaFields);
    }

    @Override
    public DirectoryReader wrap(DirectoryReader reader) {
        final Set<String> allowedMetaFields = this.allowedMetaFields;
        try {
            RequestContext context = RequestContext.current();
            if (context == null) {
                if (shardStarted == false) {
                    // The shard this index searcher wrapper has been created for hasn't started yet,
                    // We may load some initial stuff like for example previous stored percolator queries and recovery,
                    // so for this reason we should provide access to all fields:
                    return reader;
                } else {
                    logger.debug("couldn't locate the current request, field level security will only allow meta fields");
                    return FieldSubsetReader.wrap(reader, allowedMetaFields);
                }
            }

            IndicesAccessControl indicesAccessControl = context.getRequest().getFromContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
            if (indicesAccessControl == null) {
                throw Exceptions.authorizationError("no indices permissions found");
            }
            ShardId shardId = ShardUtils.extractShardId(reader);
            if (shardId == null) {
                throw new IllegalStateException(LoggerMessageFormat.format("couldn't extract shardId from reader [{}]", reader));
            }

            IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndex());
            // Either no permissions have been defined for an index or no fields have been configured for a role permission
            if (permissions == null || permissions.getFields() == null) {
                return reader;
            }

            // now add the allowed fields based on the current granted permissions and :
            Set<String> allowedFields = new HashSet<>(allowedMetaFields);
            for (String field : permissions.getFields()) {
                allowedFields.addAll(mapperService.simpleMatchToIndexNames(field));
            }
            resolveParentChildJoinFields(allowedFields);
            return FieldSubsetReader.wrap(reader, allowedFields);
        } catch (IOException e) {
            logger.error("Unable to apply field level security");
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public IndexSearcher wrap(EngineConfig engineConfig, IndexSearcher searcher) throws EngineException {
        RequestContext context = RequestContext.current();
        if (context == null) {
            if (shardStarted == false) {
                // The shard this index searcher wrapper has been created for hasn't started yet,
                // We may load some initial stuff like for example previous stored percolator queries and recovery,
                // so for this reason we should provide access to all documents:
                return searcher;
            } else {
                logger.debug("couldn't locate the current request, document level security hides all documents");
                return new ShieldIndexSearcher(engineConfig, searcher, new MatchNoDocsQuery());
            }
        }

        ShardId shardId = ShardUtils.extractShardId(searcher.getIndexReader());
        if (shardId == null) {
            throw new IllegalStateException(LoggerMessageFormat.format("couldn't extract shardId from reader [{}]", searcher.getIndexReader()));
        }
        IndicesAccessControl indicesAccessControl = context.getRequest().getFromContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            throw Exceptions.authorizationError("no indices permissions found");
        }

        IndicesAccessControl.IndexAccessControl permissions = indicesAccessControl.getIndexPermissions(shardId.getIndex());
        if (permissions == null) {
            return searcher;
        } else if (permissions.getQueries() == null) {
            return searcher;
        }

        final Query roleQuery;
        switch (permissions.getQueries().size()) {
            case 0:
                roleQuery = new MatchNoDocsQuery();
                break;
            case 1:
                roleQuery = parserService.parse(permissions.getQueries().iterator().next()).query();
                break;
            default:
                BooleanQuery bq = new BooleanQuery();
                for (BytesReference bytesReference : permissions.getQueries()) {
                    ParsedQuery parsedQuery = parserService.parse(bytesReference);
                    bq.add(parsedQuery.query(), MUST);
                }
                roleQuery = bq;
                break;
        }
        return new ShieldIndexSearcher(engineConfig, searcher, roleQuery);
    }

    public Set<String> getAllowedMetaFields() {
        return allowedMetaFields;
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

    /**
     * An {@link IndexSearcher} implementation that applies the role query for document level security during the
     * query rewrite and disabled the query cache if required when field level security is enabled.
     */
    static final class ShieldIndexSearcher extends IndexSearcher {

        private final Query roleQuery;

        private ShieldIndexSearcher(EngineConfig engineConfig, IndexSearcher in, Query roleQuery) {
            super(in.getIndexReader());
            setSimilarity(in.getSimilarity(true));
            setQueryCache(engineConfig.getQueryCache());
            setQueryCachingPolicy(engineConfig.getQueryCachingPolicy());
            this.roleQuery = roleQuery;
        }

        @Override
        public Query rewrite(Query original) throws IOException {
            return super.rewrite(wrap(original));
        }

        @Override
        public String toString() {
            return "ShieldIndexSearcher(" + super.toString() + ")";
        }

        private Query wrap(Query original) throws IOException {
            // If already wrapped, don't wrap twice:
            if (original instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) original;
                if (bq.clauses().size() == 2) {
                    Query rewrittenRoleQuery = rewrite(roleQuery);
                    if (rewrittenRoleQuery.equals(bq.clauses().get(1).getQuery())) {
                        return original;
                    }
                }
            }
            BooleanQuery bq = new BooleanQuery();
            bq.add(original, MUST);
            bq.add(roleQuery, FILTER);
            return bq;
        }
    }

    private class ShardLifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexShardPostRecovery(IndexShard indexShard) {
            if (shardId.equals(indexShard.shardId())) {
                shardStarted = true;
            }
        }
    }

}
