/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction.Request;

public class DeleteExpiredDataActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request();
        if (randomBoolean()) {
            request.setRequestsPerSecond(randomFloat());
        }
        if (randomBoolean()) {
            request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), "test"));
        }
        if (randomBoolean()) {
            request.setJobId(randomAlphaOfLength(5));
        }
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, Version version) {
        if (version.before(Version.V_7_8_0)) {
            return new Request();
        }
        if (version.before(Version.V_7_9_0)) {
            Request request = new Request();
            request.setRequestsPerSecond(instance.getRequestsPerSecond());
            request.setTimeout(instance.getTimeout());
            return request;
        }
        return instance;
    }
}
