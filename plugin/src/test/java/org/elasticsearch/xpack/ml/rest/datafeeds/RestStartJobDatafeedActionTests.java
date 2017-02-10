/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class RestStartJobDatafeedActionTests extends ESTestCase {

    public void testPrepareRequest() throws Exception {
        Job.Builder job = DatafeedJobRunnerTests.createDatafeedJob();
        DatafeedConfig datafeedConfig = DatafeedJobRunnerTests.createDatafeedConfig("foo-datafeed", "foo").build();
        RestStartDatafeedAction action = new RestStartDatafeedAction(Settings.EMPTY, mock(RestController.class));

        Map<String, String> params = new HashMap<>();
        params.put("start", "not-a-date");
        params.put("datafeed_id", "foo-datafeed");
        RestRequest restRequest1 = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
        ElasticsearchParseException e =  expectThrows(ElasticsearchParseException.class,
                () -> action.prepareRequest(restRequest1, mock(NodeClient.class)));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());

        params = new HashMap<>();
        params.put("end", "not-a-date");
        params.put("datafeed_id", "foo-datafeed");
        RestRequest restRequest2 = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
        e =  expectThrows(ElasticsearchParseException.class, () -> action.prepareRequest(restRequest2, mock(NodeClient.class)));
        assertEquals("Query param 'end' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

    public void testParseDateOrThrow() {
        assertEquals(0L, RestStartDatafeedAction.parseDateOrThrow("0", "start"));
        assertEquals(0L, RestStartDatafeedAction.parseDateOrThrow("1970-01-01T00:00:00Z", "start"));

        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> RestStartDatafeedAction.parseDateOrThrow("not-a-date", "start"));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

}
