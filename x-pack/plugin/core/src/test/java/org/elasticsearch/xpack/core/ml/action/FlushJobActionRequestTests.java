/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction.Request;

public class FlushJobActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setWaitForNormalization(randomBoolean());
        }
        if (randomBoolean()) {
            request.setRefreshRequired(randomBoolean());
        }
        if (randomBoolean()) {
            request.setCalcInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setStart(Long.toString(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setEnd(Long.toString(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setAdvanceTime(Long.toString(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setSkipTime(Long.toString(randomNonNegativeLong()));
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

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_500_012)) {
            instance.setRefreshRequired(true);
        }
        return instance;
    }
}
