/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.querydsl.container.SearchHitFieldRef;
import org.elasticsearch.xpack.eql.session.Configuration;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.HitExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ReferenceInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class SearchAfterListener implements ActionListener<SearchResponse> {

    private static final Logger log = LogManager.getLogger(SearchAfterListener.class);

    private final ActionListener<Results> listener;

    private final Client client;
    private final Configuration cfg;
    private final List<Attribute> output;
    private final QueryContainer container;
    private final SearchRequest request;

    SearchAfterListener(ActionListener<Results> listener, Client client, Configuration cfg, List<Attribute> output,
                               QueryContainer container, SearchRequest request) {

        this.listener = listener;

        this.client = client;
        this.cfg = cfg;
        this.output = output;
        this.container = container;
        this.request = request;
    }

    @Override
    public void onResponse(SearchResponse response) {
        try {
            ShardSearchFailure[] failures = response.getShardFailures();
            if (CollectionUtils.isEmpty(failures) == false) {
                listener.onFailure(new EqlIllegalArgumentException(failures[0].reason(), failures[0].getCause()));
            } else {
                handleResponse(response, listener);
            }
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private void handleResponse(SearchResponse response, ActionListener<Results> listener) {
        // create response extractors for the first time
        List<Tuple<FieldExtraction, String>> refs = container.fields();

        List<HitExtractor> exts = new ArrayList<>(refs.size());
        for (Tuple<FieldExtraction, String> ref : refs) {
            exts.add(createExtractor(ref.v1()));
        }

        if (log.isTraceEnabled()) {
            Querier.logSearchResponse(response, log);
        }

        List<?> results = Arrays.asList(response.getHits().getHits());
        listener.onResponse(new Results(response.getHits().getTotalHits(), response.getTook(), response.isTimedOut(), results));
    }

    private HitExtractor createExtractor(FieldExtraction ref) {
        if (ref instanceof SearchHitFieldRef) {
            SearchHitFieldRef f = (SearchHitFieldRef) ref;
            return new FieldHitExtractor(f.name(), f.fullFieldName(), f.getDataType(), cfg.zoneId(), f.useDocValue(), f.hitName(), false);
        }

        if (ref instanceof ComputedRef) {
            Pipe proc = ((ComputedRef) ref).processor();
            // collect hitNames
            Set<String> hitNames = new LinkedHashSet<>();
            proc = proc.transformDown(l -> {
                HitExtractor he = createExtractor(l.context());
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

    @Override
    public void onFailure(Exception ex) {
        listener.onFailure(ex);
    }
}