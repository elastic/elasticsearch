/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link DeleteSampleConfigurationAction} and its inner {@link DeleteSampleConfigurationAction.Request} class.
 * <p>
 * This test class verifies serialization/deserialization, action properties, request validation,
 * and proper behavior of the delete sampling configuration action.
 * </p>
 */
public class DeleteSampleConfigurationActionTests extends AbstractWireSerializingTestCase<DeleteSampleConfigurationAction.Request> {

    @Override
    protected Writeable.Reader<DeleteSampleConfigurationAction.Request> instanceReader() {
        return DeleteSampleConfigurationAction.Request::new;
    }

    @Override
    protected DeleteSampleConfigurationAction.Request createTestInstance() {
        return createRandomRequest();
    }

    @Override
    protected DeleteSampleConfigurationAction.Request mutateInstance(DeleteSampleConfigurationAction.Request instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                // Mutate indices
                DeleteSampleConfigurationAction.Request mutated = new DeleteSampleConfigurationAction.Request(
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                String[] newIndices = randomArrayOtherThan(instance.indices(), () -> new String[] { randomAlphaOfLength(10) });
                mutated.indices(newIndices);
                yield mutated;
            }
            case 1 -> {
                // Mutate master node timeout
                DeleteSampleConfigurationAction.Request mutated = new DeleteSampleConfigurationAction.Request(
                    randomValueOtherThan(instance.masterNodeTimeout(), () -> TimeValue.timeValueSeconds(randomIntBetween(1, 60))),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 2 -> {
                // Mutate acknowledgment timeout
                DeleteSampleConfigurationAction.Request mutated = new DeleteSampleConfigurationAction.Request(
                    instance.masterNodeTimeout(),
                    randomValueOtherThan(instance.ackTimeout(), () -> TimeValue.timeValueSeconds(randomIntBetween(1, 60)))
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    /**
     * Creates a random DeleteSampleConfigurationAction.Request for testing.
     *
     * @return a randomly configured request instance
     */
    private DeleteSampleConfigurationAction.Request createRandomRequest() {
        DeleteSampleConfigurationAction.Request request = new DeleteSampleConfigurationAction.Request(
            TimeValue.timeValueSeconds(randomIntBetween(1, 60)),
            TimeValue.timeValueSeconds(randomIntBetween(1, 60))
        );

        request.indices(randomAlphaOfLength(10));

        return request;
    }

    /**
     * Test that the action name is correctly defined.
     */
    public void testActionName() {
        assertThat(DeleteSampleConfigurationAction.NAME, equalTo("indices:admin/sample/config/delete"));
        assertThat(DeleteSampleConfigurationAction.INSTANCE.name(), equalTo(DeleteSampleConfigurationAction.NAME));
    }

    /**
     * Test that the action instance is properly implemented as a singleton.
     */
    public void testActionInstance() {
        assertThat(DeleteSampleConfigurationAction.INSTANCE, notNullValue());
        assertThat(DeleteSampleConfigurationAction.INSTANCE, sameInstance(DeleteSampleConfigurationAction.INSTANCE));
    }

    /**
     * Test that the request has the correct indices options and data stream support.
     */
    public void testRequestIndicesOptions() {
        DeleteSampleConfigurationAction.Request request = createRandomRequest();
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS));
        assertThat(request.includeDataStreams(), is(true));
    }

}
