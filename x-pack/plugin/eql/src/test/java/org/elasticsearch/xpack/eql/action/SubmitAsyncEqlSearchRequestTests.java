/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.core.eql.action.SubmitAsyncEqlSearchRequest;
import org.elasticsearch.xpack.core.transform.action.AbstractWireSerializingTransformTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SubmitAsyncEqlSearchRequestTests extends AbstractWireSerializingTransformTestCase<SubmitAsyncEqlSearchRequest> {
    @Override
    protected Writeable.Reader<SubmitAsyncEqlSearchRequest> instanceReader() {
        return SubmitAsyncEqlSearchRequest::new;
    }

    @Override
    protected SubmitAsyncEqlSearchRequest createTestInstance() {
        final SubmitAsyncEqlSearchRequest searchRequest;
        if (randomBoolean()) {
            searchRequest = new SubmitAsyncEqlSearchRequest(generateRandomStringArray(10, 10, false, false));
        } else {
            searchRequest = new SubmitAsyncEqlSearchRequest();
        }
        searchRequest.getEqlSearchRequest().query(randomAlphaOfLength(10));
        if (randomBoolean()) {
            searchRequest.setWaitForCompletionTimeout(TimeValue.parseTimeValue(randomPositiveTimeValue(), "wait_for_completion"));
        }
        searchRequest.setKeepOnCompletion(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setKeepAlive(TimeValue.parseTimeValue(randomPositiveTimeValue(), "keep_alive"));
        }
        if (randomBoolean()) {
            searchRequest.getEqlSearchRequest()
                .indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.getEqlSearchRequest().eventCategoryField(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.getEqlSearchRequest().fetchSize(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            searchRequest.getEqlSearchRequest().filter(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        return searchRequest;
    }

    public void testValidateKeepAlive() {
        SubmitAsyncEqlSearchRequest req = new SubmitAsyncEqlSearchRequest(
            new EqlSearchRequest().indices(randomAlphaOfLength(10)).query(randomAlphaOfLength(10)));
        req.setKeepAlive(TimeValue.timeValueSeconds(randomIntBetween(1, 59)));
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(1));
        assertThat(exc.validationErrors().get(0), containsString("[keep_alive]"));
    }

}
