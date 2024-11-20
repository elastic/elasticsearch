/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlClientException;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.extractor.CompositeKeyExtractor;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.querydsl.container.CompositeAggRef;
import org.elasticsearch.xpack.eql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.eql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.HitExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ReferenceInput;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.util.Queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueSupport.FULL;
import static org.elasticsearch.xpack.ql.util.Queries.Clause.FILTER;

public final class RuntimeUtils {

    static final Logger QUERY_LOG = LogManager.getLogger(QueryClient.class);

    private RuntimeUtils() {}

    public static ActionListener<SearchResponse> searchLogListener(ActionListener<SearchResponse> listener, Logger log) {
        return listener.delegateFailureAndWrap((delegate, response) -> {
            ShardSearchFailure[] failures = response.getShardFailures();
            if (CollectionUtils.isEmpty(failures) == false) {
                delegate.onFailure(new EqlIllegalArgumentException(failures[0].reason(), failures[0].getCause()));
                return;
            }
            if (log.isTraceEnabled()) {
                logSearchResponse(response, log);
            }
            delegate.onResponse(response);
        });
    }

    public static ActionListener<MultiSearchResponse> multiSearchLogListener(ActionListener<MultiSearchResponse> listener, Logger log) {
        return listener.delegateFailureAndWrap((delegate, items) -> {
            for (MultiSearchResponse.Item item : items) {
                Exception failure = item.getFailure();
                SearchResponse response = item.getResponse();

                if (failure == null) {
                    ShardSearchFailure[] failures = response.getShardFailures();
                    if (CollectionUtils.isEmpty(failures) == false) {
                        failure = new EqlIllegalArgumentException(failures[0].reason(), failures[0].getCause());
                    }
                }
                if (failure != null) {
                    delegate.onFailure(failure);
                    return;
                }
                if (log.isTraceEnabled()) {
                    logSearchResponse(response, log);
                }
            }
            delegate.onResponse(items);
        });
    }

    private static void logSearchResponse(SearchResponse response, Logger logger) {
        List<InternalAggregation> aggs = Collections.emptyList();
        if (response.getAggregations() != null) {
            aggs = response.getAggregations().asList();
        }
        StringBuilder aggsNames = new StringBuilder();
        for (int i = 0; i < aggs.size(); i++) {
            aggsNames.append(aggs.get(i).getName() + (i + 1 == aggs.size() ? "" : ", "));
        }

        SearchHit[] hits = response.getHits().getHits();
        int count = hits != null ? hits.length : 0;
        logger.trace(
            "Got search response [hits {}, {} aggregations: [{}], {} failed shards, {} skipped shards, "
                + "{} successful shards, {} total shards, took {}, timed out [{}]]",
            count,
            aggs.size(),
            aggsNames,
            response.getFailedShards(),
            response.getSkippedShards(),
            response.getSuccessfulShards(),
            response.getTotalShards(),
            response.getTook(),
            response.isTimedOut()
        );
    }

    public static List<HitExtractor> createExtractor(List<FieldExtraction> fields, EqlConfiguration cfg) {
        List<HitExtractor> extractors = new ArrayList<>(fields.size());

        for (FieldExtraction fe : fields) {
            extractors.add(createExtractor(fe, cfg));
        }
        return extractors;
    }

    public static BucketExtractor createBucketExtractor(FieldExtraction ref) {
        if (ref instanceof CompositeAggRef aggRef) {
            return new CompositeKeyExtractor(aggRef.key(), false);
        } else if (ref instanceof ComputedRef computedRef) {
            Pipe proc = computedRef.processor();
            String hitName = Expressions.name(proc.expression());
            return new ComputingExtractor(proc.asProcessor(), hitName);
        }
        throw new EqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
    }

    public static HitExtractor createExtractor(FieldExtraction ref, EqlConfiguration cfg) {
        if (ref instanceof SearchHitFieldRef f) {
            return new FieldHitExtractor(f.name(), f.getDataType(), cfg.zoneId(), f.hitName(), FULL);
        }

        if (ref instanceof ComputedRef computedRef) {
            Pipe proc = computedRef.processor();
            // collect hitNames
            Set<String> hitNames = new LinkedHashSet<>();
            proc = proc.transformDown(ReferenceInput.class, l -> {
                HitExtractor he = createExtractor(l.context(), cfg);
                hitNames.add(he.hitName());

                if (hitNames.size() > 1) {
                    throw new EqlClientException("Multi-level nested fields [{}] not supported yet", hitNames);
                }

                return new HitExtractorInput(l.source(), l.expression(), he);
            });
            String hitName = null;
            if (hitNames.size() == 1) {
                hitName = hitNames.iterator().next();
            }
            return new ComputingExtractor(proc.asProcessor(), hitName);
        }

        throw new EqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
    }

    public static SearchRequest prepareRequest(SearchSourceBuilder source, boolean includeFrozen, String... indices) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);
        searchRequest.source(source);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.indicesOptions(
            includeFrozen ? IndexResolver.FIELD_CAPS_FROZEN_INDICES_OPTIONS : IndexResolver.FIELD_CAPS_INDICES_OPTIONS
        );
        return searchRequest;
    }

    /**
     * optimized method that adds filter to existing bool queries without additional wrapping
     * additionally checks whether the given query exists for safe decoration
     */
    public static SearchSourceBuilder combineFilters(SearchSourceBuilder source, QueryBuilder filter) {
        var query = Queries.combine(FILTER, Arrays.asList(source.query(), filter));
        query = query == null ? boolQuery() : query;
        return source.query(query);
    }

    public static SearchSourceBuilder replaceFilter(
        SearchSourceBuilder source,
        List<QueryBuilder> oldFilters,
        List<QueryBuilder> newFilters
    ) {
        var query = source.query();
        query = removeFilters(query, oldFilters);
        query = Queries.combine(
            FILTER,
            org.elasticsearch.xpack.ql.util.CollectionUtils.combine(Collections.singletonList(query), newFilters)
        );
        query = query == null ? boolQuery() : query;
        return source.query(query);
    }

    public static SearchSourceBuilder wrapAsFilter(SearchSourceBuilder source) {
        QueryBuilder query = source.query();
        BoolQueryBuilder bool = boolQuery();
        if (query != null) {
            bool.filter(query);
        }

        source.query(bool);
        return source;
    }

    public static QueryBuilder removeFilters(QueryBuilder query, List<QueryBuilder> filters) {
        if (query instanceof BoolQueryBuilder boolQueryBuilder) {
            if (org.elasticsearch.xpack.ql.util.CollectionUtils.isEmpty(filters) == false) {
                boolQueryBuilder.filter().removeAll(filters);
            }
        }
        return query;
    }
}
