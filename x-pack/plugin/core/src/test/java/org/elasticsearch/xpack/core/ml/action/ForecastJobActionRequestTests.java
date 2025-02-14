/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction.Request;

import static org.hamcrest.Matchers.equalTo;

public class ForecastJobActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setDuration(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)).getStringRep());
        }
        if (randomBoolean()) {
            request.setExpiresIn(TimeValue.timeValueSeconds(randomIntBetween(0, 1_000_000)).getStringRep());
        }
        if (randomBoolean()) {
            request.setMaxModelMemory(
                randomLongBetween(ByteSizeValue.of(1, ByteSizeUnit.MB).getBytes(), ByteSizeValue.of(499, ByteSizeUnit.MB).getBytes())
            );
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testSetDuration_GivenZero() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setDuration("0"));
        assertThat(e.getMessage(), equalTo("[duration] must be positive: [0ms]"));
    }

    public void testSetDuration_GivenNegative() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setDuration("-1s"));
        assertThat(e.getMessage(), equalTo("[duration] must be positive: [-1]"));
    }

    public void testSetExpiresIn_GivenZero() {
        Request request = new Request();
        request.setExpiresIn("0");
        assertThat(request.getExpiresIn(), equalTo(TimeValue.ZERO));
    }

    public void testSetExpiresIn_GivenNegative() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setExpiresIn("-1s"));
        assertThat(e.getMessage(), equalTo("[expires_in] must be non-negative: [-1]"));
    }
}
