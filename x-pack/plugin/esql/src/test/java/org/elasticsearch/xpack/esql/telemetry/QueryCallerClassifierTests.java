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
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

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

    public void testKnownProductOrigins() {
        assertThat(QueryCallerClassifier.clientFromOrigin("fleet"), is(QueryMetric.FLEET));
        assertThat(QueryCallerClassifier.clientFromOrigin("Logstash"), is(QueryMetric.LOGSTASH));
        assertThat(QueryCallerClassifier.clientFromOrigin("observability"), is(QueryMetric.OBSERVABILITY));
        assertThat(QueryCallerClassifier.clientFromOrigin("connectors"), is(QueryMetric.CONNECTORS));
        assertThat(QueryCallerClassifier.clientFromOrigin("elastic-synthetics"), is(QueryMetric.ELASTIC_SYNTHETICS));
    }

    public void testKnownProductOriginWithSuffix() {
        assertThat(QueryCallerClassifier.clientFromOrigin("logstash-output-elasticsearch"), is(QueryMetric.LOGSTASH));
        assertThat(QueryCallerClassifier.clientFromOrigin("logstash-filter-elasticsearch"), is(QueryMetric.LOGSTASH));
        assertThat(QueryCallerClassifier.clientFromOrigin("Fleet-Server"), is(QueryMetric.FLEET));
    }

    public void testUnknownOriginsAreOther() {
        assertThat(QueryCallerClassifier.clientFromOrigin("kibanaish"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("logstashx"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("beats"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("elastic"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("-kibana"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("rest"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin("other"), is(QueryMetric.OTHER));
        assertThat(QueryCallerClassifier.clientFromOrigin(randomAlphaOfLengthBetween(1, 5) + "_" + randomUUID()), is(QueryMetric.OTHER));
    }

    public void testEveryKnownProductOriginHasItsOwnClient() {
        Set<String> productClients = Arrays.stream(QueryMetric.values())
            .filter(client -> client != QueryMetric.OTHER && client != QueryMetric.REST)
            .map(QueryMetric::toString)
            .collect(Collectors.toSet());
        assertThat(productClients, equalTo(MetricAttributes.KNOWN_PRODUCT_ORIGINS));
        for (String origin : MetricAttributes.KNOWN_PRODUCT_ORIGINS) {
            assertThat(QueryCallerClassifier.clientFromOrigin(origin).toString(), equalTo(origin));
        }
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
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.FLEET)));
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "my-app");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.OTHER)));
        }
    }

    public void testClassifyWithoutThreadContextIsRest() {
        assertThat(QueryCallerClassifier.classify(null), equalTo(new QueryCaller(QueryMetric.REST)));
    }
}
