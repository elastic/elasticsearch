/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.eql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.HitExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ReferenceInput;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class RuntimeUtils {

    static final Logger QUERY_LOG = LogManager.getLogger(QueryClient.class);

    private RuntimeUtils() {}

    static void logSearchResponse(SearchResponse response, Logger logger) {
        List<Aggregation> aggs = Collections.emptyList();
        if (response.getAggregations() != null) {
            aggs = response.getAggregations().asList();
        }
        StringBuilder aggsNames = new StringBuilder();
        for (int i = 0; i < aggs.size(); i++) {
            aggsNames.append(aggs.get(i).getName() + (i + 1 == aggs.size() ? "" : ", "));
        }

        logger.trace("Got search response [hits {} {}, {} aggregations: [{}], {} failed shards, {} skipped shards, "
                + "{} successful shards, {} total shards, took {}, timed out [{}]]", response.getHits().getTotalHits().relation.toString(),
                response.getHits().getTotalHits().value, aggs.size(), aggsNames, response.getFailedShards(), response.getSkippedShards(),
                response.getSuccessfulShards(), response.getTotalShards(), response.getTook(), response.isTimedOut());
    }

    public static List<HitExtractor> createExtractor(List<FieldExtraction> fields, EqlConfiguration cfg) {
        List<HitExtractor> extractors = new ArrayList<>(fields.size());

        for (FieldExtraction fe : fields) {
            extractors.add(createExtractor(fe, cfg));
        }
        return extractors;
    }
    
    public static HitExtractor createExtractor(FieldExtraction ref, EqlConfiguration cfg) {
        if (ref instanceof SearchHitFieldRef) {
            SearchHitFieldRef f = (SearchHitFieldRef) ref;
            return new FieldHitExtractor(f.name(), f.fullFieldName(), f.getDataType(), cfg.zoneId(), f.useDocValue(), f.hitName(), false);
        }

        if (ref instanceof ComputedRef) {
            Pipe proc = ((ComputedRef) ref).processor();
            // collect hitNames
            Set<String> hitNames = new LinkedHashSet<>();
            proc = proc.transformDown(l -> {
                HitExtractor he = createExtractor(l.context(), cfg);
                hitNames.add(he.hitName());

                if (hitNames.size() > 1) {
                    throw new EqlIllegalArgumentException("Multi-level nested fields [{}] not supported yet", hitNames);
                }

                return new HitExtractorInput(l.source(), l.expression(), he);
            }, ReferenceInput.class);
            String hitName = null;
            if (hitNames.size() == 1) {
                hitName = hitNames.iterator().next();
            }
            return new ComputingExtractor(proc.asProcessor(), hitName);
        }

        throw new EqlIllegalArgumentException("Unexpected value reference {}", ref.getClass());
    }
    

    public static SearchRequest prepareRequest(Client client,
                                               SearchSourceBuilder source,
                                               boolean includeFrozen,
                                               String... indices) {
        return client.prepareSearch(indices)
                .setSource(source)
                .setAllowPartialSearchResults(false)
                .setIndicesOptions(
                        includeFrozen ? IndexResolver.FIELD_CAPS_FROZEN_INDICES_OPTIONS : IndexResolver.FIELD_CAPS_INDICES_OPTIONS)
                .request();
    }
}