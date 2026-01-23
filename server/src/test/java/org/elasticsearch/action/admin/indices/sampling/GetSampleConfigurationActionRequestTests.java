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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetSampleConfigurationActionRequestTests extends ESTestCase {

    protected GetSampleConfigurationAction.Request createTestInstance() {
        GetSampleConfigurationAction.Request request = new GetSampleConfigurationAction.Request(randomBoundedTimeValue());
        request.indices(randomAlphaOfLength(10));
        return request;
    }

    protected GetSampleConfigurationAction.Request mutateInstance(GetSampleConfigurationAction.Request instance) {
        // Since Request.equals() only compares the index field (not masterTimeout),
        // we must mutate the index to ensure the mutated instance is not equal
        String newIndex = randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(1, 10));
        GetSampleConfigurationAction.Request mutated = new GetSampleConfigurationAction.Request(
            randomBoolean() ? instance.masterTimeout() : randomBoundedTimeValue()
        );
        mutated.indices(newIndex);
        return mutated;
    }

    public void testRequestValidation() {
        // Valid request
        GetSampleConfigurationAction.Request validRequest = new GetSampleConfigurationAction.Request(randomBoundedTimeValue());
        validRequest.indices("test-index");
        assertThat(validRequest.validate(), nullValue());

        // Invalid request with null index
        GetSampleConfigurationAction.Request invalidRequest = new GetSampleConfigurationAction.Request(randomBoundedTimeValue());
        ActionRequestValidationException validation = invalidRequest.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("index name is required"));

        // Invalid request with empty index
        GetSampleConfigurationAction.Request emptyIndexRequest = new GetSampleConfigurationAction.Request(randomBoundedTimeValue());
        emptyIndexRequest.indices("");
        validation = emptyIndexRequest.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("index name is required"));
    }

    public void testEqualsAndHashcode() {
        for (int i = 0; i < 10; i++) {
            GetSampleConfigurationAction.Request request1 = createTestInstance();
            GetSampleConfigurationAction.Request request2 = new GetSampleConfigurationAction.Request(request1.masterTimeout());
            request2.indices(request1.getIndex());

            // Test equality
            assertThat(request1, equalTo(request2));
            assertThat(request1.hashCode(), equalTo(request2.hashCode()));

            // Test mutation creates non-equal instance
            GetSampleConfigurationAction.Request mutated = mutateInstance(request1);
            assertThat("Mutation should create non-equal instance", request1, not(equalTo(mutated)));

            // Test reflexivity
            assertThat(request1, equalTo(request1));

            // Test null and different class
            assertThat(request1.equals(null), equalTo(false));
            assertThat(request1.equals("not a request"), equalTo(false));
        }
    }

    public void testGettersAndSetters() {
        String indexName = randomAlphaOfLength(10);
        GetSampleConfigurationAction.Request request = new GetSampleConfigurationAction.Request(randomBoundedTimeValue());
        request.indices(indexName);

        assertThat(request.getIndex(), equalTo(indexName));
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(indexName));
    }

    public void testIncludeDataStreams() {
        GetSampleConfigurationAction.Request request = createTestInstance();
        assertThat(request.includeDataStreams(), equalTo(true));
    }

    private TimeValue randomBoundedTimeValue() {
        return TimeValue.timeValueSeconds(randomIntBetween(5, 10));
    }
}
