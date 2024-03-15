/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collections;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestGetStackTracesActionTests extends RestActionTestCase {
    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetStackTracesAction());
    }

    public void testPrepareEmptyRequest() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GetStackTracesRequest.class));
            GetStackTracesRequest getStackTracesRequest = (GetStackTracesRequest) request;
            assertThat(getStackTracesRequest.getSampleSize(), is(20_000)); // expect the default value
            assertThat(getStackTracesRequest.getQuery(), nullValue());
            executeCalled.set(true);
            return new GetStackTracesResponse(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                0,
                1.0d,
                0L
            );
        });
        RestRequest profilingRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_profiling/stacktraces")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(profilingRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testPrepareParameterizedRequest() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GetStackTracesRequest.class));
            GetStackTracesRequest getStackTracesRequest = (GetStackTracesRequest) request;
            assertThat(getStackTracesRequest.getSampleSize(), is(10_000));
            assertThat(getStackTracesRequest.getRequestedDuration(), is(3_600.0d));
            assertThat(getStackTracesRequest.getAwsCostFactor(), is(1.0d));
            assertThat(getStackTracesRequest.getAzureCostFactor(), is(1.1d));
            assertThat(getStackTracesRequest.getCustomCO2PerKWH(), is(0.005d));
            assertThat(getStackTracesRequest.getCustomDatacenterPUE(), is(1.5d));
            assertThat(getStackTracesRequest.getCustomPerCoreWattX86(), is(7.5d));
            assertThat(getStackTracesRequest.getCustomPerCoreWattARM64(), is(2.0d));
            assertThat(getStackTracesRequest.getCustomCostPerCoreHour(), is(0.083d));
            assertThat(getStackTracesRequest.getQuery(), notNullValue(QueryBuilder.class));
            executeCalled.set(true);

            GetStackTracesResponseBuilder responseBuilder = new GetStackTracesResponseBuilder(getStackTracesRequest);
            responseBuilder.setSamplingRate(0.04d);
            responseBuilder.setTotalFrames(523);
            responseBuilder.setTotalSamples(3L);

            GetStackTracesResponse response = responseBuilder.build();
            assertNull(response.getStackTraces());
            assertNull(response.getStackFrames());
            assertNull(response.getExecutables());
            assertNull(response.getStackTraceEvents());
            assertEquals(response.getSamplingRate(), 0.04d, 0.0001d);
            assertEquals(response.getTotalFrames(), 523);
            assertEquals(response.getTotalSamples(), 3L);

            return response;
        });
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_profiling/stacktraces")
            .withContent(new BytesArray("""
                            {
                              "sample_size": 10000,
                              "requested_duration": 3600,
                              "aws_cost_factor": 1.0,
                              "azure_cost_factor": 1.1,
                              "co2_per_kwh": 0.005,
                              "datacenter_pue": 1.5,
                              "per_core_watt_x86": 7.5,
                              "per_core_watt_arm64": 2.0,
                              "cost_per_core_hour": 0.083,
                              "query": {
                                "bool": {
                                  "filter": [
                                    {
                                      "range": {
                                        "@timestamp": {
                                          "gte": "2022-10-05",
                                          "lt": "2022-12-05"
                                        }
                                      }
                                    }
                                  ]
                                }
                              }
                            }
                """), XContentType.JSON)
            .build();
        dispatchRequest(request);
        assertThat(executeCalled.get(), equalTo(true));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        // to register the query parser
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }
}
