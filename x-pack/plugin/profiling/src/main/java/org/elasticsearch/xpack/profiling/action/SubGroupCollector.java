/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.Iterator;

public final class SubGroupCollector {
    /**
     * Users may provide a custom field via the API that is used to sub-divide profiling events. This is useful in the context of TopN
     * where we want to provide additional breakdown of where a certain function has been called (e.g. a certain service or transaction).
     */
    static final String CUSTOM_EVENT_SUB_AGGREGATION_NAME = "custom_event_group_";

    private static final Logger log = LogManager.getLogger(SubGroupCollector.class);

    private final String[] aggregationFields;

    public static SubGroupCollector attach(AbstractAggregationBuilder<?> parentAggregation, String[] aggregationFields) {
        SubGroupCollector c = new SubGroupCollector(aggregationFields);
        c.addAggregations(parentAggregation);
        return c;
    }

    private SubGroupCollector(String[] aggregationFields) {
        this.aggregationFields = aggregationFields;
    }

    private boolean hasAggregationFields() {
        return aggregationFields != null && aggregationFields.length > 0;
    }

    private void addAggregations(AbstractAggregationBuilder<?> parentAggregation) {
        if (hasAggregationFields()) {
            // cast to Object to disambiguate this from a varargs call
            log.trace("Grouping stacktrace events by {}.", (Object) aggregationFields);
            AbstractAggregationBuilder<?> parentAgg = parentAggregation;
            for (String aggregationField : aggregationFields) {
                String aggName = CUSTOM_EVENT_SUB_AGGREGATION_NAME + aggregationField;
                TermsAggregationBuilder agg = new TermsAggregationBuilder(aggName).field(aggregationField);
                parentAgg.subAggregation(agg);
                parentAgg = agg;
            }
        }
    }

    void collectResults(MultiBucketsAggregation.Bucket bucket, TraceEvent event) {
        collectResults(new BucketAdapter(bucket), event);
    }

    void collectResults(Bucket bucket, TraceEvent event) {
        if (hasAggregationFields()) {
            if (event.subGroups == null) {
                event.subGroups = SubGroup.root(aggregationFields[0]);
            }
            collectInternal(bucket.getAggregations(), event.subGroups, 0);
        }
    }

    private void collectInternal(Agg parentAgg, SubGroup parentGroup, int aggField) {
        if (aggField == aggregationFields.length) {
            return;
        }
        String aggName = CUSTOM_EVENT_SUB_AGGREGATION_NAME + aggregationFields[aggField];
        for (Bucket b : parentAgg.getBuckets(aggName)) {
            String subGroupName = b.getKey();
            parentGroup.addCount(subGroupName, b.getCount());
            SubGroup currentGroup = parentGroup.getSubGroup(subGroupName);
            int nextAggField = aggField + 1;
            if (nextAggField < aggregationFields.length) {
                collectInternal(b.getAggregations(), currentGroup.getOrAddChild(aggregationFields[nextAggField]), nextAggField);
            }
        }
    }

    // The sole purpose of the code below is to abstract our code from the aggs framework to make it unit-testable
    interface Agg {
        Iterable<Bucket> getBuckets(String aggName);

    }

    interface Bucket {
        String getKey();

        long getCount();

        Agg getAggregations();
    }

    static class InternalAggregationAdapter implements Agg {
        private final InternalAggregations agg;

        InternalAggregationAdapter(InternalAggregations agg) {
            this.agg = agg;
        }

        @Override
        public Iterable<Bucket> getBuckets(String aggName) {
            MultiBucketsAggregation multiBucketsAggregation = agg.get(aggName);
            return () -> {
                Iterator<? extends MultiBucketsAggregation.Bucket> it = multiBucketsAggregation.getBuckets().iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Bucket next() {
                        return new BucketAdapter(it.next());
                    }
                };
            };
        }
    }

    static class BucketAdapter implements Bucket {
        private final MultiBucketsAggregation.Bucket bucket;

        BucketAdapter(MultiBucketsAggregation.Bucket bucket) {
            this.bucket = bucket;
        }

        @Override
        public String getKey() {
            return bucket.getKeyAsString();
        }

        @Override
        public long getCount() {
            return bucket.getDocCount();
        }

        @Override
        public Agg getAggregations() {
            return new InternalAggregationAdapter(bucket.getAggregations());
        }
    }
}
