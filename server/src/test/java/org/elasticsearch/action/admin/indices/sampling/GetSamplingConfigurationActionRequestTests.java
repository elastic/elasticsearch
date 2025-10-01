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
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetSamplingConfigurationActionRequestTests extends AbstractWireSerializingTestCase<GetSamplingConfigurationAction.Request> {

    @Override
    protected Writeable.Reader<GetSamplingConfigurationAction.Request> instanceReader() {
        return GetSamplingConfigurationAction.Request::new;
    }

    @Override
    protected GetSamplingConfigurationAction.Request createTestInstance() {
        return new GetSamplingConfigurationAction.Request(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected GetSamplingConfigurationAction.Request mutateInstance(GetSamplingConfigurationAction.Request instance) {
        return new GetSamplingConfigurationAction.Request(
            randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(1, 20))
        );
    }

    public void testRequestValidation() {
        // Valid request
        GetSamplingConfigurationAction.Request validRequest = new GetSamplingConfigurationAction.Request("test-index");
        assertThat(validRequest.validate(), nullValue());

        // Invalid request with null index
        GetSamplingConfigurationAction.Request invalidRequest = new GetSamplingConfigurationAction.Request((String) null);
        ActionRequestValidationException validation = invalidRequest.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("index name is required"));

        // Invalid request with empty index
        GetSamplingConfigurationAction.Request emptyIndexRequest = new GetSamplingConfigurationAction.Request("");
        ActionRequestValidationException emptyValidation = emptyIndexRequest.validate();
        assertThat(emptyValidation, notNullValue());
        assertThat(emptyValidation.getMessage(), containsString("index name is required"));
    }

    public void testRequestIndices() {
        String indexName = "test-index";
        GetSamplingConfigurationAction.Request request = new GetSamplingConfigurationAction.Request(indexName);

        assertThat(request.indices(), equalTo(new String[] { indexName }));
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.strictSingleIndexNoExpandForbidClosed()));
        assertThat(request.includeDataStreams(), equalTo(true));
    }

    public void testRequestIndicesReplacement() {
        GetSamplingConfigurationAction.Request request = new GetSamplingConfigurationAction.Request("original-index");

        // Valid replacement with single index
        request.indices("new-index");
        assertThat(request.getIndex(), equalTo("new-index"));
        assertThat(request.indices(), equalTo(new String[] { "new-index" }));

        // Invalid replacement with multiple indices should throw exception
        expectThrows(IllegalArgumentException.class, () -> request.indices("index1", "index2"));

        // Invalid replacement with no indices should throw exception
        expectThrows(IllegalArgumentException.class, () -> request.indices(new String[0]));

        // Invalid replacement with null should throw exception
        expectThrows(IllegalArgumentException.class, () -> request.indices((String[]) null));
    }
}
