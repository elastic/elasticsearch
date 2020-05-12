/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.listener.RuntimeUtils;
import org.elasticsearch.xpack.eql.execution.payload.Payload;
import org.elasticsearch.xpack.eql.execution.payload.SearchResponsePayload;
import org.elasticsearch.xpack.eql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.execution.search.extractor.TimestampFieldHitExtractor;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.eql.querydsl.container.FieldExtractorRegistry;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.eql.execution.listener.RuntimeUtils.prepareRequest;

public class ExecutionManager implements QueryClient {

    private static final Logger log = LogManager.getLogger(ExecutionManager.class);

    private final EqlConfiguration cfg;
    private final Client client;
    private final TimeValue keepAlive;
    private final String indices;

    public ExecutionManager(EqlSession eqlSession) {
        this.cfg = eqlSession.configuration();
        this.client = eqlSession.client();
        this.keepAlive = cfg.requestTimeout();
        this.indices = cfg.indexAsWildcard();
    }

    public Executable from(SequenceExec seqExec) {
        FieldExtractorRegistry extractorRegistry = new FieldExtractorRegistry();
        
        List<List<Attribute>> listOfKeys = seqExec.keys();
        List<PhysicalPlan> plans = seqExec.children();
        List<Criterion> criteria = new ArrayList<>(plans.size() - 1);
        
        // build a criterion for each query
        for (int i = 0; i < plans.size() - 1; i++) {
            List<Attribute> keys = listOfKeys.get(i);
            // fields
            HitExtractor tsExtractor = timestampExtractor(hitExtractor(seqExec.timestamp(), extractorRegistry));
            List<HitExtractor> keyExtractors = hitExtractors(keys, extractorRegistry);

            PhysicalPlan query = plans.get(i);
            // search query
            // TODO: this could be generalized into an exec only query
            Check.isTrue(query instanceof EsQueryExec, "Expected a query but got [{}]", query.getClass());
            QueryContainer container = ((EsQueryExec) query).queryContainer();
            SearchSourceBuilder searchSource = SourceGenerator.sourceBuilder(container, cfg.filter(), cfg.size());
            
            criteria.add(new Criterion(searchSource, keyExtractors, tsExtractor));
        }
        return new SequenceRuntime(criteria, this);
    }

    private HitExtractor timestampExtractor(HitExtractor hitExtractor) {
        if (hitExtractor instanceof FieldHitExtractor) {
            FieldHitExtractor fe = (FieldHitExtractor) hitExtractor;
            return (fe instanceof TimestampFieldHitExtractor) ? hitExtractor : new TimestampFieldHitExtractor(fe);
        }
        throw new EqlIllegalArgumentException("Unexpected extractor [{}]", hitExtractor);
    }

    private HitExtractor hitExtractor(Expression exp, FieldExtractorRegistry registry) {
        return RuntimeUtils.createExtractor(registry.fieldExtraction(exp), cfg);
    }

    private List<HitExtractor> hitExtractors(List<? extends Expression> exps, FieldExtractorRegistry registry) {
        List<HitExtractor> extractors = new ArrayList<>(exps.size());
        for (Expression exp : exps) {
            extractors.add(hitExtractor(exp, registry));
        }
        return extractors;
    }

    @Override
    public void query(SearchSourceBuilder searchSource, ActionListener<Payload<SearchHit>> listener) {
        // set query timeout
        searchSource.timeout(cfg.requestTimeout());

        if (log.isTraceEnabled()) {
            log.trace("About to execute query {} on {}", StringUtils.toString(searchSource), indices);
        }
        if (cfg.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        SearchRequest search = prepareRequest(client, searchSource, false, indices);
        client.search(search, wrap(sr -> listener.onResponse(new SearchResponsePayload(sr)), listener::onFailure));
    }
}