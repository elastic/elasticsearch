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
        return new Request(
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), "test")
        );
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
        return instance;
    }
}
