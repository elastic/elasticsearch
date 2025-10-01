/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DeleteSamplingConfigurationActionTests extends AbstractWireSerializingTestCase<DeleteSamplingConfigurationAction.Request> {

    @Override
    protected Writeable.Reader<DeleteSamplingConfigurationAction.Request> instanceReader() {
        return DeleteSamplingConfigurationAction.Request::new;
    }

    @Override
    protected DeleteSamplingConfigurationAction.Request createTestInstance() {
        return createRandomRequest();
    }

    @Override
    protected DeleteSamplingConfigurationAction.Request mutateInstance(DeleteSamplingConfigurationAction.Request instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                // Mutate indices
                DeleteSamplingConfigurationAction.Request mutated = new DeleteSamplingConfigurationAction.Request(
                    instance.masterNodeTimeout(),
                    instance.ackTimeout(),
                    randomValueOtherThan(
                        instance.indices(),
                        () -> randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(1, 10))
                    )
                );
                yield mutated;
            }
            case 1 -> {
                // Mutate master node timeout
                DeleteSamplingConfigurationAction.Request mutated = new DeleteSamplingConfigurationAction.Request(
                    randomValueOtherThan(instance.masterNodeTimeout(), () -> TimeValue.timeValueSeconds(randomIntBetween(1, 60))),
                    instance.ackTimeout(),
                    instance.indices()
                );
                yield mutated;
            }
            case 2 -> {
                // Mutate ack timeout
                DeleteSamplingConfigurationAction.Request mutated = new DeleteSamplingConfigurationAction.Request(
                    instance.masterNodeTimeout(),
                    randomValueOtherThan(instance.ackTimeout(), () -> TimeValue.timeValueSeconds(randomIntBetween(1, 60))),
                    instance.indices()
                );
                yield mutated;
            }
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    private DeleteSamplingConfigurationAction.Request createRandomRequest() {
        String[] indices = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(1, 10));
        TimeValue masterNodeTimeout = TimeValue.timeValueSeconds(randomIntBetween(1, 60));
        TimeValue ackTimeout = TimeValue.timeValueSeconds(randomIntBetween(1, 60));

        return new DeleteSamplingConfigurationAction.Request(masterNodeTimeout, ackTimeout, indices);
    }

    public void testActionName() {
        assertThat(DeleteSamplingConfigurationAction.NAME, equalTo("indices:admin/sampling/config/delete"));
        assertThat(DeleteSamplingConfigurationAction.INSTANCE.name(), equalTo(DeleteSamplingConfigurationAction.NAME));
    }

    public void testActionInstance() {
        assertThat(DeleteSamplingConfigurationAction.INSTANCE, notNullValue());
        assertThat(DeleteSamplingConfigurationAction.INSTANCE, sameInstance(DeleteSamplingConfigurationAction.INSTANCE));
    }

    public void testRequestValidation() {
        // Valid request
        DeleteSamplingConfigurationAction.Request validRequest = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10),
            "test-index"
        );
        assertThat(validRequest.validate(), nullValue());

        // Invalid request with no indices
        DeleteSamplingConfigurationAction.Request noIndicesRequest = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10)
        );
        ActionRequestValidationException validation = noIndicesRequest.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("at least one index name is required"));

        // Invalid request with empty indices array
        DeleteSamplingConfigurationAction.Request emptyIndicesRequest = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10),
            new String[0]
        );
        ActionRequestValidationException emptyValidation = emptyIndicesRequest.validate();
        assertThat(emptyValidation, notNullValue());
        assertThat(emptyValidation.getMessage(), containsString("at least one index name is required"));

        // Invalid request with null index in array
        DeleteSamplingConfigurationAction.Request nullIndexRequest = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10),
            "valid",
            null,
            "also-valid"
        );
        ActionRequestValidationException nullValidation = nullIndexRequest.validate();
        assertThat(nullValidation, notNullValue());
        assertThat(nullValidation.getMessage(), containsString("index name cannot be null or empty"));

        // Invalid request with empty index in array
        DeleteSamplingConfigurationAction.Request emptyIndexRequest = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10),
            "valid",
            "",
            "also-valid"
        );
        ActionRequestValidationException emptyIndexValidation = emptyIndexRequest.validate();
        assertThat(emptyIndexValidation, notNullValue());
        assertThat(emptyIndexValidation.getMessage(), containsString("index name cannot be null or empty"));
    }

    public void testRequestIndicesOptions() {
        DeleteSamplingConfigurationAction.Request request = createRandomRequest();
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED));
        assertThat(request.includeDataStreams(), is(true));
    }

    public void testMultipleIndicesSupport() {
        String[] indices = { "logs", "metrics", "traces", "data-stream-1" };
        DeleteSamplingConfigurationAction.Request request = new DeleteSamplingConfigurationAction.Request(
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(10),
            indices
        );

        assertThat(request.indices(), equalTo(indices));
        assertThat(request.validate(), nullValue());
        assertThat(request.includeDataStreams(), is(true));
    }
}
