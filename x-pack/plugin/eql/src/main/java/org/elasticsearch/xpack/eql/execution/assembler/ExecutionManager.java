/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.PITAwareQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.eql.execution.search.extractor.TimestampFieldHitExtractor;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceMatcher;
import org.elasticsearch.xpack.eql.execution.sequence.TumblingWindow;
import org.elasticsearch.xpack.eql.expression.OptionalResolvedAttribute;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.querydsl.container.FieldExtractorRegistry;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public class ExecutionManager {

    private final EqlSession session;
    private final EqlConfiguration cfg;

    public ExecutionManager(EqlSession eqlSession) {
        this.session = eqlSession;
        this.cfg = eqlSession.configuration();
    }

    public Executable assemble(
        List<List<Attribute>> listOfKeys,
        List<PhysicalPlan> plans,
        Attribute timestamp,
        Attribute tiebreaker,
        OrderDirection direction,
        TimeValue maxSpan,
        Limit limit
    ) {
        FieldExtractorRegistry extractorRegistry = new FieldExtractorRegistry();

        boolean descending = direction == OrderDirection.DESC;

        // fields
        HitExtractor tsExtractor = timestampExtractor(hitExtractor(timestamp, extractorRegistry));
        HitExtractor tbExtractor = Expressions.isPresent(tiebreaker) ? hitExtractor(tiebreaker, extractorRegistry) : null;
        // implicit tiebreake, present only in the response and which doesn't have a corresponding field
        HitExtractor itbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;
        // NB: since there's no aliasing inside EQL, the attribute name is the same as the underlying field name
        String timestampName = Expressions.name(timestamp);

        // secondary criteria
        List<Criterion<BoxedQueryRequest>> criteria = new ArrayList<>(plans.size() - 1);

        // build a criterion for each query
        for (int i = 0; i < plans.size(); i++) {
            List<Attribute> keys = listOfKeys.get(i);
            List<HitExtractor> keyExtractors = hitExtractors(keys, extractorRegistry);
            List<String> keyFields = new ArrayList<>(keyExtractors.size());

            Set<String> optionalKeys = new LinkedHashSet<>(CollectionUtils.mapSize(keyExtractors.size()));

            // extract top-level fields used as keys to optimize query lookups
            // this process gets skipped for nested fields
            for (int j = 0; j < keyExtractors.size(); j++) {
                HitExtractor extractor = keyExtractors.get(j);

                if (extractor instanceof AbstractFieldHitExtractor hitExtractor) {
                    // remember if the field is optional
                    boolean isOptional = keys.get(j) instanceof OptionalResolvedAttribute;
                    // no nested fields
                    if (hitExtractor.hitName() == null) {
                        String fieldName = hitExtractor.fieldName();
                        keyFields.add(fieldName);
                        if (isOptional) {
                            optionalKeys.add(fieldName);
                        }
                    } else {
                        keyFields = emptyList();
                        break;
                    }
                    // optional field
                } else if (extractor instanceof ComputingExtractor computingExtractor) {
                    keyFields.add(computingExtractor.hitName());
                }
            }

            PhysicalPlan query = plans.get(i);
            // search query
            if (query instanceof EsQueryExec esQueryExec) {
                SearchSourceBuilder source = esQueryExec.source(session, false);
                QueryRequest original = () -> source;
                BoxedQueryRequest boxedRequest = new BoxedQueryRequest(original, timestampName, keyFields, optionalKeys);
                Criterion<BoxedQueryRequest> criterion = new Criterion<>(
                    i,
                    boxedRequest,
                    keyExtractors,
                    tsExtractor,
                    tbExtractor,
                    itbExtractor,
                    i == 0 && descending
                );
                criteria.add(criterion);
            } else {
                // until
                if (i != plans.size() - 1) {
                    throw new EqlIllegalArgumentException("Expected a query but got [{}]", query.getClass());
                } else {
                    criteria.add(null);
                }
            }
        }

        int completionStage = criteria.size() - 1;
        SequenceMatcher matcher = new SequenceMatcher(completionStage, descending, maxSpan, limit, session.circuitBreaker());

        TumblingWindow w = new TumblingWindow(
            new PITAwareQueryClient(session),
            criteria.subList(0, completionStage),
            criteria.get(completionStage),
            matcher
        );

        return w;
    }

    private HitExtractor timestampExtractor(HitExtractor hitExtractor) {
        if (hitExtractor instanceof FieldHitExtractor fe) {
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
}
