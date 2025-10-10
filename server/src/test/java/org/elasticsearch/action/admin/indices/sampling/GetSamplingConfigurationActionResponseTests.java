/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GetSamplingConfigurationActionResponseTests extends AbstractWireSerializingTestCase<GetSamplingConfigurationAction.Response> {

    @Override
    protected Writeable.Reader<GetSamplingConfigurationAction.Response> instanceReader() {
        return GetSamplingConfigurationAction.Response::new;
    }

    @Override
    protected GetSamplingConfigurationAction.Response createTestInstance() {
        return createRandomResponse();
    }

    @Override
    protected GetSamplingConfigurationAction.Response mutateInstance(GetSamplingConfigurationAction.Response instance) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new GetSamplingConfigurationAction.Response(
                randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(1, 20)),
                instance.getConfiguration()
            );
            case 1 -> new GetSamplingConfigurationAction.Response(
                instance.getIndex(),
                randomValueOtherThan(instance.getConfiguration(), this::createRandomSamplingConfiguration)
            );
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    private GetSamplingConfigurationAction.Response createRandomResponse() {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        SamplingConfiguration config = randomBoolean() ? null : createRandomSamplingConfiguration();
        return new GetSamplingConfigurationAction.Response(indexName, config);
    }

    private SamplingConfiguration createRandomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofGb(randomLongBetween(1, SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    public void testActionName() {
        assertThat(GetSamplingConfigurationAction.NAME, equalTo("indices:admin/sampling/config/get"));
        assertThat(GetSamplingConfigurationAction.INSTANCE.name(), equalTo(GetSamplingConfigurationAction.NAME));
    }

    public void testActionInstance() {
        assertThat(GetSamplingConfigurationAction.INSTANCE, notNullValue());
        assertThat(GetSamplingConfigurationAction.INSTANCE, sameInstance(GetSamplingConfigurationAction.INSTANCE));
    }

    public void testResponseWithNullConfiguration() throws IOException {
        String indexName = "test-index";
        GetSamplingConfigurationAction.Response response = new GetSamplingConfigurationAction.Response(indexName, null);

        // Test serialization/deserialization
        GetSamplingConfigurationAction.Response deserialized = copyInstance(response);
        assertThat(deserialized, equalTo(response));
        assertThat(deserialized.getIndex(), equalTo(indexName));
        assertThat(deserialized.getConfiguration(), nullValue());
    }

    public void testResponseWithConfiguration() throws IOException {
        String indexName = "test-index";
        SamplingConfiguration config = createRandomSamplingConfiguration();
        GetSamplingConfigurationAction.Response response = new GetSamplingConfigurationAction.Response(indexName, config);

        // Test serialization/deserialization
        GetSamplingConfigurationAction.Response deserialized = copyInstance(response);
        assertThat(deserialized, equalTo(response));
        assertThat(deserialized.getIndex(), equalTo(indexName));
        assertThat(deserialized.getConfiguration(), equalTo(config));
    }

}
