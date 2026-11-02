/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class QueryCallerClassifierTests extends ESTestCase {

    public void testNoOriginIsRest() {
        assertThat(QueryCallerClassifier.clientFromOrigin(null), is(QueryMetric.REST));
        assertThat(QueryCallerClassifier.clientFromOrigin(""), is(QueryMetric.REST));
        assertThat(QueryCallerClassifier.clientFromOrigin("   "), is(QueryMetric.REST));
    }

    public void testKibanaOrigins() {
        assertThat(QueryCallerClassifier.clientFromOrigin("kibana"), is(QueryMetric.KIBANA));
        assertThat(QueryCallerClassifier.clientFromOrigin("Kibana"), is(QueryMetric.KIBANA));
        assertThat(QueryCallerClassifier.clientFromOrigin(" kibana "), is(QueryMetric.KIBANA));
        assertThat(QueryCallerClassifier.clientFromOrigin("kibana-fleet"), is(QueryMetric.KIBANA));
        assertThat(QueryCallerClassifier.clientFromOrigin("KIBANA-Agent-Builder"), is(QueryMetric.KIBANA));
    }

    public void testOtherElasticProductOrigins() {
        assertThat(QueryCallerClassifier.clientFromOrigin("kibanaish"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("fleet"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("cloud"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin(randomAlphaOfLengthBetween(1, 5) + "-" + randomUUID()), is(QueryMetric.OTHER));
    }

    public void testClassifyReadsProductOriginHeader() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.REST)));
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.KIBANA)));
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "fleet");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.OTHER)));
        }
    }

    public void testClassifyWithoutThreadContextIsRest() {
        assertThat(QueryCallerClassifier.classify(null), equalTo(new QueryCaller(QueryMetric.REST)));
    }
}
