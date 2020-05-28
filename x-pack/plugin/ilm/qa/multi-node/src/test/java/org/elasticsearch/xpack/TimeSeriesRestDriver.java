/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.Step;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * This class provides the operational REST functions needed to control an ILM time series lifecycle.
 */
public final class TimeSeriesRestDriver {

    private static final Logger logger = LogManager.getLogger(TimeSeriesRestDriver.class);

    private TimeSeriesRestDriver() {
    }

    public static Step.StepKey getStepKeyForIndex(RestClient client, String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(client, indexName);
        if (indexResponse == null) {
            return new Step.StepKey(null, null, null);
        }

        return getStepKey(indexResponse);
    }

    private static Step.StepKey getStepKey(Map<String, Object> explainIndexResponse) {
        String phase = (String) explainIndexResponse.get("phase");
        String action = (String) explainIndexResponse.get("action");
        String step = (String) explainIndexResponse.get("step");
        return new Step.StepKey(phase, action, step);
    }

    public static Map<String, Object> explainIndex(RestClient client, String indexName) throws IOException {
        return explain(client, indexName, false, false).get(indexName);
    }

    public static Map<String, Map<String, Object>> explain(RestClient client, String indexPattern, boolean onlyErrors,
                                                           boolean onlyManaged) throws IOException {
        Request explainRequest = new Request("GET", indexPattern + "/_ilm/explain");
        explainRequest.addParameter("only_errors", Boolean.toString(onlyErrors));
        explainRequest.addParameter("only_managed", Boolean.toString(onlyManaged));
        Response response = client.performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> indexResponse =
            ((Map<String, Map<String, Object>>) responseMap.get("indices"));
        return indexResponse;
    }

    public static void indexDocument(RestClient client, String indexAbstractionName) throws IOException {
        indexDocument(client, indexAbstractionName, false);
    }

    public static void indexDocument(RestClient client, String indexAbstractionName, boolean refresh) throws IOException {
        Request indexRequest = new Request("POST", indexAbstractionName + "/_doc" + (refresh ? "?refresh" : ""));
        indexRequest.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
        Response response = client.performRequest(indexRequest);
        logger.info(response.getStatusLine());
    }

    public static void createNewSingletonPolicy(RestClient client, String policyName, String phaseName, LifecycleAction action)
        throws IOException {
        createNewSingletonPolicy(client, policyName, phaseName, action, TimeValue.ZERO);
    }

    public static void createNewSingletonPolicy(RestClient client, String policyName, String phaseName, LifecycleAction action,
                                                TimeValue after) throws IOException {
        Phase phase = new Phase(phaseName, after, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        client.performRequest(request);
    }

    public static void createComposableTemplate(RestClient client, String templateName, String indexPattern, Template template)
        throws IOException {
        XContentBuilder builder = jsonBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        StringEntity templateJSON = new StringEntity(
            String.format(Locale.ROOT, "{\n" +
                "  \"index_patterns\": \"%s\",\n" +
                "  \"data_stream\": { \"timestamp_field\": \"@timestamp\" },\n" +
                "  \"template\": %s\n" +
                "}", indexPattern, Strings.toString(builder)),
            ContentType.APPLICATION_JSON);
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

}
