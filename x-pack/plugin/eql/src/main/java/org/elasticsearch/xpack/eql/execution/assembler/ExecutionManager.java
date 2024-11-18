/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.sample.SampleIterator;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.PITAwareQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.execution.search.extractor.CompositeKeyExtractor;
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
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.wrapAsFilter;

public class ExecutionManager {

    private static final int SAMPLE_MAX_PAGE_SIZE = 1000;
    private final EqlSession session;
    private final EqlConfiguration cfg;

    public ExecutionManager(EqlSession eqlSession) {
        this.session = eqlSession;
        this.cfg = eqlSession.configuration();
    }

    /*
     * Sequence assembler
     */
    public Executable assemble(
        List<List<Attribute>> listOfKeys,
        List<PhysicalPlan> plans,
        Attribute timestamp,
        Attribute tiebreaker,
        OrderDirection direction,
        TimeValue maxSpan,
        Limit limit,
        boolean[] missing
    ) {
        FieldExtractorRegistry extractorRegistry = new FieldExtractorRegistry();

        boolean descending = direction == OrderDirection.DESC;

        // fields
        HitExtractor tsExtractor = timestampExtractor(hitExtractor(timestamp, extractorRegistry));
        HitExtractor tbExtractor = Expressions.isPresent(tiebreaker) ? hitExtractor(tiebreaker, extractorRegistry) : null;
        // implicit tiebreaker, present only in the response and which doesn't have a corresponding field
        HitExtractor itbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;
        // NB: since there's no aliasing inside EQL, the attribute name is the same as the underlying field name
        String timestampName = Expressions.name(timestamp);

        // secondary criteria
        List<SequenceCriterion> criteria = new ArrayList<>(plans.size() - 1);

        int firstPositive = -1;
        for (int i = 0; i < missing.length; i++) {
            if (missing[i] == false) {
                firstPositive = i;
                break;
            }
        }
        // build a criterion for each query
        for (int i = 0; i < plans.size(); i++) {
            List<Attribute> keys = listOfKeys.get(i);
            List<HitExtractor> keyExtractors = hitExtractors(keys, extractorRegistry);
            List<String> keyFields = new ArrayList<>(keyExtractors.size());

            Set<String> optionalKeys = Sets.newLinkedHashSetWithExpectedSize(CollectionUtils.mapSize(keyExtractors.size()));

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
                SequenceCriterion criterion = new SequenceCriterion(
                    i,
                    boxedRequest,
                    keyExtractors,
                    tsExtractor,
                    tbExtractor,
                    itbExtractor,
                    i == firstPositive && descending,
                    missing[i]
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
        SequenceMatcher matcher = new SequenceMatcher(
            completionStage,
            descending,
            maxSpan,
            limit,
            toMissing(criteria.subList(0, criteria.size() - 1)), // no need for "until" here
            session.circuitBreaker()
        );

        TumblingWindow w = new TumblingWindow(
            new PITAwareQueryClient(session),
            criteria.subList(0, completionStage),
            criteria.get(completionStage),
            matcher,
            listOfKeys,
            cfg.allowPartialSearchResults(),
            cfg.allowPartialSequenceResults()
        );

        return w;
    }

    /*
     * Sample assembler
     */
    public Executable assemble(List<List<Attribute>> listOfKeys, List<PhysicalPlan> plans, Limit limit) {
        if (cfg.fetchSize() > SAMPLE_MAX_PAGE_SIZE) {
            throw new InvalidArgumentException("Fetch size cannot be greater than [{}]", SAMPLE_MAX_PAGE_SIZE);
        }

        FieldExtractorRegistry extractorRegistry = new FieldExtractorRegistry();
        List<SampleCriterion> criteria = new ArrayList<>(plans.size() - 1);

        // build a criterion for each query
        for (int i = 0; i < plans.size(); i++) {
            List<Attribute> keys = listOfKeys.get(i);
            List<BucketExtractor> keyExtractors = compositeKeyExtractors(keys, extractorRegistry);
            List<String> keyFields = new ArrayList<>(keyExtractors.size());

            for (int j = 0; j < keyExtractors.size(); j++) {
                BucketExtractor extractor = keyExtractors.get(j);
                if (extractor instanceof CompositeKeyExtractor e) {
                    keyFields.add(e.key());
                } else if (extractor instanceof ComputingExtractor ce) {
                    keyFields.add(ce.hitName());
                }
            }

            PhysicalPlan query = plans.get(i);
            // search query
            if (query instanceof EsQueryExec esQueryExec) {
                SampleQueryRequest firstQuery = new SampleQueryRequest(
                    () -> wrapAsFilter(esQueryExec.source(session, false)),
                    keyFields,
                    keys,
                    cfg.fetchSize()
                );
                SampleQueryRequest midQuery = new SampleQueryRequest(
                    () -> wrapAsFilter(esQueryExec.source(session, false)),
                    keyFields,
                    keys,
                    cfg.fetchSize()
                );
                SampleQueryRequest finalQuery = new SampleQueryRequest(
                    () -> wrapAsFilter(esQueryExec.source(session, false)),
                    keyFields,
                    keys,
                    cfg.fetchSize()
                );
                firstQuery.withCompositeAggregation();
                midQuery.withCompositeAggregation();

                criteria.add(new SampleCriterion(firstQuery, midQuery, finalQuery, keyFields, keyExtractors));
            } else {
                throw new EqlIllegalArgumentException("Expected a query but got [{}]", query.getClass());
            }
        }

        return new SampleIterator(
            new PITAwareQueryClient(session),
            criteria,
            cfg.fetchSize(),
            limit,
            session.circuitBreaker(),
            cfg.maxSamplesPerKey(),
            cfg.allowPartialSearchResults()
        );
    }

    private static boolean[] toMissing(List<SequenceCriterion> criteria) {
        boolean[] result = new boolean[criteria.size()];
        for (int i = 0; i < criteria.size(); i++) {
            result[i] = criteria.get(i).missing();
        }
        return result;
    }

    private static HitExtractor timestampExtractor(HitExtractor hitExtractor) {
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

    private static List<BucketExtractor> compositeKeyExtractors(List<? extends Expression> exps, FieldExtractorRegistry registry) {
        List<BucketExtractor> extractors = new ArrayList<>(exps.size());
        for (Expression exp : exps) {
            extractors.add(RuntimeUtils.createBucketExtractor(registry.compositeKeyExtraction(exp)));
        }
        return extractors;
    }
}
