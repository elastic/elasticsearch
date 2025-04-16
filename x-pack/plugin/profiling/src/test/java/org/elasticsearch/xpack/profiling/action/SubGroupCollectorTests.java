/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.profiling.action.SubGroupCollector.CUSTOM_EVENT_SUB_AGGREGATION_NAME;

public class SubGroupCollectorTests extends ESTestCase {
    public void testNoAggs() {
        TermsAggregationBuilder stackTraces = new TermsAggregationBuilder("stacktraces").field("stacktrace.id");
        TraceEvent traceEvent = new TraceEvent(1L);

        SubGroupCollector collector = SubGroupCollector.attach(stackTraces, new String[0]);
        assertTrue("Sub aggregations attached", stackTraces.getSubAggregations().isEmpty());

        SubGroupCollector.Bucket currentStackTrace = bucket("1", 5);
        collector.collectResults(currentStackTrace, traceEvent);

        assertNull(traceEvent.subGroups);
    }

    public void testMultipleAggsInSingleStackTrace() {
        TermsAggregationBuilder stackTraces = new TermsAggregationBuilder("stacktraces").field("stacktrace.id");
        TraceEvent traceEvent = new TraceEvent(1L);

        SubGroupCollector collector = SubGroupCollector.attach(stackTraces, new String[] { "service.name", "transaction.name" });
        assertFalse("No sub aggregations attached", stackTraces.getSubAggregations().isEmpty());

        StaticAgg services = new StaticAgg();
        SubGroupCollector.Bucket currentStackTrace = bucket("1", 5, services);
        // tag::noformat
        services.addBuckets(CUSTOM_EVENT_SUB_AGGREGATION_NAME + "service.name",
            bucket("basket", 7L,
                agg(CUSTOM_EVENT_SUB_AGGREGATION_NAME + "transaction.name",
                    bucket("add-to-basket", 4L),
                    bucket("delete-from-basket", 3L)
                )
            ),
            bucket("checkout", 4L,
                agg(CUSTOM_EVENT_SUB_AGGREGATION_NAME + "transaction.name",
                    bucket("enter-address", 4L),
                    bucket("submit-order", 3L)
                )
            )
        );
        // end::noformat

        collector.collectResults(currentStackTrace, traceEvent);

        assertNotNull(traceEvent.subGroups);
        assertEquals(Long.valueOf(7L), traceEvent.subGroups.getCount("basket"));
        assertEquals(Long.valueOf(4L), traceEvent.subGroups.getCount("checkout"));
        SubGroup basketTransactionNames = traceEvent.subGroups.getSubGroup("basket").getSubGroup("transaction.name");
        assertEquals(Long.valueOf(4L), basketTransactionNames.getCount("add-to-basket"));
        assertEquals(Long.valueOf(3L), basketTransactionNames.getCount("delete-from-basket"));
        SubGroup checkoutTransactionNames = traceEvent.subGroups.getSubGroup("checkout").getSubGroup("transaction.name");
        assertEquals(Long.valueOf(4L), checkoutTransactionNames.getCount("enter-address"));
        assertEquals(Long.valueOf(3L), checkoutTransactionNames.getCount("submit-order"));
    }

    public void testSingleAggInMultipleStackTraces() {
        TermsAggregationBuilder stackTraces = new TermsAggregationBuilder("stacktraces").field("stacktrace.id");
        TraceEvent traceEvent = new TraceEvent(1L);

        SubGroupCollector collector = SubGroupCollector.attach(stackTraces, new String[] { "service.name" });
        assertFalse("No sub aggregations attached", stackTraces.getSubAggregations().isEmpty());

        StaticAgg services1 = new StaticAgg();
        SubGroupCollector.Bucket currentStackTrace1 = bucket("1", 5, services1);
        services1.addBuckets(CUSTOM_EVENT_SUB_AGGREGATION_NAME + "service.name", bucket("basket", 7L));

        collector.collectResults(currentStackTrace1, traceEvent);

        StaticAgg services2 = new StaticAgg();
        SubGroupCollector.Bucket currentStackTrace2 = bucket("1", 3, services2);
        services2.addBuckets(CUSTOM_EVENT_SUB_AGGREGATION_NAME + "service.name", bucket("basket", 1L), bucket("checkout", 5L));

        collector.collectResults(currentStackTrace2, traceEvent);

        assertNotNull(traceEvent.subGroups);
        assertEquals(Long.valueOf(8L), traceEvent.subGroups.getCount("basket"));
        assertEquals(Long.valueOf(5L), traceEvent.subGroups.getCount("checkout"));
    }

    private SubGroupCollector.Bucket bucket(String key, long count) {
        return bucket(key, count, null);
    }

    private SubGroupCollector.Bucket bucket(String key, long count, SubGroupCollector.Agg aggregations) {
        return new StaticBucket(key, count, aggregations);
    }

    private SubGroupCollector.Agg agg(String name, SubGroupCollector.Bucket... buckets) {
        StaticAgg a = new StaticAgg();
        a.addBuckets(name, buckets);
        return a;
    }

    private static class StaticBucket implements SubGroupCollector.Bucket {
        private final String key;
        private final long count;
        private SubGroupCollector.Agg aggregations;

        private StaticBucket(String key, long count, SubGroupCollector.Agg aggregations) {
            this.key = key;
            this.count = count;
            this.aggregations = aggregations;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public SubGroupCollector.Agg getAggregations() {
            return aggregations;
        }
    }

    private static class StaticAgg implements SubGroupCollector.Agg {
        private final Map<String, List<SubGroupCollector.Bucket>> buckets = new HashMap<>();

        public void addBuckets(String name, SubGroupCollector.Bucket... buckets) {
            this.buckets.put(name, List.of(buckets));
        }

        @Override
        public Iterable<SubGroupCollector.Bucket> getBuckets(String aggName) {
            return buckets.get(aggName);
        }
    }
}
