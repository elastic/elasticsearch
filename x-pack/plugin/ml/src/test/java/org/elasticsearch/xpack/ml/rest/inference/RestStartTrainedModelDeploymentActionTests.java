/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.FULLY_ALLOCATED;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus.State.STARTING;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestStartTrainedModelDeploymentActionTests extends RestActionTestCase {
    private final TestCase testCase;

    public RestStartTrainedModelDeploymentActionTests(TestCase testCase) {
        this.testCase = testCase;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        List<TestCase> testCases = List.of(
            // parsing from body only
            TestCase.of(
                "Parses body timeout field",
                false,
                (description, request) -> assertThat(description, request.getTimeout(), is(TimeValue.timeValueSeconds(4))),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("timeout", "4s").endObject()
            ),
            TestCase.of(
                "Parses body wait_for state field",
                false,
                (description, request) -> assertThat(description, request.getWaitForState(), is(FULLY_ALLOCATED)),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("wait_for", FULLY_ALLOCATED.toString()).endObject()
            ),
            TestCase.of(
                "Parses body number_of_allocations field",
                false,
                (description, request) -> assertThat(description, request.getNumberOfAllocations(), is(2)),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("number_of_allocations", "2").endObject()
            ),
            TestCase.of(
                "Parses body threads_per_allocation field",
                false,
                (description, request) -> assertThat(description, request.getThreadsPerAllocation(), is(2)),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("threads_per_allocation", "2").endObject()
            ),
            TestCase.of(
                "Parses body queue_capacity field",
                false,
                (description, request) -> assertThat(description, request.getQueueCapacity(), is(2)),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("queue_capacity", "2").endObject()
            ),
            TestCase.of(
                "Parses body cache_size field",
                false,
                (description, request) -> assertThat(description, request.getCacheSize(), is(ByteSizeValue.ofMb(2))),
                Map.of(),
                XContentFactory.jsonBuilder().startObject().field("cache_size", "2mb").endObject()
            ),
            // parsing from query params only
            TestCase.of(
                "Parses query param timeout field",
                false,
                (description, request) -> assertThat(description, request.getTimeout(), is(TimeValue.timeValueSeconds(4))),
                Map.of("timeout", "4s")
            ),
            TestCase.of(
                "Parses query param wait_for state field",
                false,
                (description, request) -> assertThat(description, request.getWaitForState(), is(FULLY_ALLOCATED)),
                Map.of("wait_for", FULLY_ALLOCATED.toString())
            ),
            TestCase.of(
                "Parses query param number_of_allocations field",
                false,
                (description, request) -> assertThat(description, request.getNumberOfAllocations(), is(2)),
                Map.of("number_of_allocations", "2")
            ),
            TestCase.of(
                "Parses query param threads_per_allocation field",
                false,
                (description, request) -> assertThat(description, request.getThreadsPerAllocation(), is(2)),
                Map.of("threads_per_allocation", "2")
            ),
            TestCase.of(
                "Parses query param queue_capacity field",
                false,
                (description, request) -> assertThat(description, request.getQueueCapacity(), is(2)),
                Map.of("queue_capacity", "2")
            ),
            TestCase.of(
                "Parses query param cache_size field",
                false,
                (description, request) -> assertThat(description, request.getCacheSize(), is(ByteSizeValue.ofMb(2))),
                Map.of("cache_size", "2mb")
            ),
            // query params override body
            TestCase.of(
                "Query param overrides body timeout field",
                false,
                (description, request) -> assertThat(description, request.getTimeout(), is(TimeValue.timeValueSeconds(4))),
                Map.of("timeout", "4s"),
                XContentFactory.jsonBuilder().startObject().field("timeout", "2s").endObject()
            ),
            TestCase.of(
                "Query param overrides body wait_for state field",
                false,
                (description, request) -> assertThat(description, request.getWaitForState(), is(STARTING)),
                Map.of("wait_for", STARTING.toString()),
                XContentFactory.jsonBuilder().startObject().field("wait_for", FULLY_ALLOCATED.toString()).endObject()
            ),
            TestCase.of(
                "Query param overrides body number_of_allocations field",
                false,
                (description, request) -> assertThat(description, request.getNumberOfAllocations(), is(5)),
                Map.of("number_of_allocations", "5"),
                XContentFactory.jsonBuilder().startObject().field("number_of_allocations", "2").endObject()
            ),
            TestCase.of(
                "Query param overrides body threads_per_allocation field",
                false,
                (description, request) -> assertThat(description, request.getThreadsPerAllocation(), is(3)),
                Map.of("threads_per_allocation", "3"),
                XContentFactory.jsonBuilder().startObject().field("threads_per_allocation", "2").endObject()
            ),
            TestCase.of(
                "Query param overrides body queue_capacity field",
                false,
                (description, request) -> assertThat(description, request.getQueueCapacity(), is(2)),
                Map.of("queue_capacity", "2"),
                XContentFactory.jsonBuilder().startObject().field("queue_capacity", "1").endObject()
            ),
            TestCase.of(
                "Query param overrides body cache_size field",
                false,
                (description, request) -> assertThat(description, request.getCacheSize(), is(ByteSizeValue.ofMb(3))),
                Map.of("cache_size", "3mb"),
                XContentFactory.jsonBuilder().startObject().field("cache_size", "2mb").endObject()
            ),
            // cache size tests
            TestCase.of(
                "Disables cache_size",
                true,
                (description, request) -> assertThat(description, request.getCacheSize(), is(ByteSizeValue.ZERO)),
                Map.of()
            ),
            TestCase.of(
                "Sets cache_size to null",
                false,
                (description, request) -> assertNull(description, request.getCacheSize()),
                Map.of()
            )
        );

        return testCases.stream().map(TestCase::toArray).collect(toList());
    }

    /**
     * This test is run for each of the supplied {@link TestCase} configurations.
     * @throws IOException _
     */
    public void test() throws IOException {
        controller().registerHandler(testCase.action);
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(StartTrainedModelDeploymentAction.Request.class));

            var request = (StartTrainedModelDeploymentAction.Request) actionRequest;
            testCase.verifyingAssertFunc.accept(testCase.testDescription, request);

            executeCalled.set(true);
            return createResponse();
        }));

        dispatchRequest(testCase.buildRequestFunc.apply(xContentRegistry()));
        assertThat(testCase.testDescription, executeCalled.get(), equalTo(true));
    }

    private static CreateTrainedModelAssignmentAction.Response createResponse() {
        return new CreateTrainedModelAssignmentAction.Response(TrainedModelAssignmentTests.randomInstance());
    }

    /**
     * A single test case
     * @param testDescription description of the test
     * @param action the rest action specifying whether the cache should be disabled
     * @param verifyingAssertFunc an assertion function that will be called after the
     *                            {@link RestStartTrainedModelDeploymentAction#prepareRequest} method is called
     * @param buildRequestFunc a function for constructing a fake request
     */
    public record TestCase(
        String testDescription,
        RestStartTrainedModelDeploymentAction action,
        BiConsumer<String, StartTrainedModelDeploymentAction.Request> verifyingAssertFunc,
        Function<NamedXContentRegistry, RestRequest> buildRequestFunc
    ) {
        private static TestCase of(
            String testDescription,
            boolean shouldDisableCache,
            BiConsumer<String, StartTrainedModelDeploymentAction.Request> verifyingAssertFunc,
            Map<String, String> queryParams,
            @Nullable XContentBuilder builder
        ) {
            return new TestCase(
                testDescription,
                new RestStartTrainedModelDeploymentAction(shouldDisableCache),
                verifyingAssertFunc,
                buildRequest(queryParams, builder)
            );
        }

        private static TestCase of(
            String testDescription,
            boolean shouldDisableCache,
            BiConsumer<String, StartTrainedModelDeploymentAction.Request> verifyingAssertFunc,
            Map<String, String> queryParams
        ) {
            return of(testDescription, shouldDisableCache, verifyingAssertFunc, queryParams, null);
        }

        private static Function<NamedXContentRegistry, RestRequest> buildRequest(Map<String, String> queryParams, XContentBuilder builder) {
            Map<String, String> params = new HashMap<>(Map.of("model_id", "model", "deployment_id", "dep"));
            params.putAll(queryParams);

            return (registry) -> {
                var requestBuilder = new FakeRestRequest.Builder(registry).withMethod(RestRequest.Method.POST)
                    .withPath("_ml/trained_models/test_id/deployment/_start")
                    .withParams(params);

                if (builder != null) {
                    requestBuilder = requestBuilder.withContent(BytesReference.bytes(builder), XContentType.JSON);
                }

                return requestBuilder.build();
            };
        }

        Object[] toArray() {
            return new Object[] { this };
        }
    }

}
