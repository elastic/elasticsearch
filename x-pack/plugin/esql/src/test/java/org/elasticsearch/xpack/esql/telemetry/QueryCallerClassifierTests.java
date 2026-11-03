/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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
        assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.REST, null)));
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.OTHER_KIBANA)));
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "fleet");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.FLEET, null)));
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "my-app");
            assertThat(QueryCallerClassifier.classify(threadContext), equalTo(new QueryCaller(QueryMetric.OTHER, null)));
        }
    }

    public void testClassifyWithoutThreadContextIsRest() {
        assertThat(QueryCallerClassifier.classify(null), equalTo(new QueryCaller(QueryMetric.REST, null)));
    }

    public void testKibanaCallerCarriesTheExecutionContextFeature() {
        assertThat(
            QueryCallerClassifier.classify(threadContext("kibana", "6b6a3bb3;kibana:application:discover:new")),
            equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.DISCOVER))
        );
        assertThat(
            QueryCallerClassifier.classify(
                threadContext("Kibana", "unknownId;kibana:task%20manager:run%20alerting_v2%3Adispatcher:alerting_v2%3Adispatcher%3A1.0.0")
            ),
            equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.ALERTING_V2))
        );
        assertThat(
            QueryCallerClassifier.classify(threadContext("kibana-reporting", "x;kibana:task%20manager:run%20report%3Aexecute:t")),
            equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.REPORTING))
        );
    }

    public void testKibanaCallerWithoutExecutionContextIsOtherKibana() {
        assertThat(
            QueryCallerClassifier.classify(threadContext("kibana", null)),
            equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.OTHER_KIBANA))
        );
        assertThat(
            QueryCallerClassifier.classify(threadContext("kibana", randomUUID())),
            equalTo(new QueryCaller(QueryMetric.KIBANA, Feature.OTHER_KIBANA))
        );
    }

    public void testOnlyKibanaCallersHaveAKibanaFeature() {
        String opaqueId = "6b6a3bb3;kibana:application:discover:new";
        for (String origin : Arrays.asList(null, "", "fleet", "logstash-output-elasticsearch", "my-app", "kibanaish")) {
            QueryCaller caller = QueryCallerClassifier.classify(threadContext(origin, opaqueId));
            assertThat(String.valueOf(origin), caller.client(), not(QueryMetric.KIBANA));
            assertThat(String.valueOf(origin), caller.kibanaFeature(), nullValue());
        }
    }

    private static ThreadContext threadContext(@Nullable String origin, @Nullable String opaqueId) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (origin != null) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, origin);
        }
        if (opaqueId != null) {
            threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
        }
        return threadContext;
    }
}
