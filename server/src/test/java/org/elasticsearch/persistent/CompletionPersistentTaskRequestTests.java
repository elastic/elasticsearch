/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.CompletionPersistentTaskAction.Request;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class CompletionPersistentTaskRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        if (randomBoolean()) {
            return new Request(randomTimeValue(), randomAlphaOfLength(10), randomNonNegativeLong(), null, null);
        } else {
            return new Request(randomTimeValue(), randomAlphaOfLength(10), randomNonNegativeLong(), null, randomAlphaOfLength(20));
        }
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String taskId = instance.getTaskId();
        long allocationId = instance.getAllocationId();
        Exception exception = instance.getException();
        String localAbortReason = instance.getLocalAbortReason();
        // A task cannot be both failed and locally aborted, so only add an exception when there is no local abort reason
        switch (between(0, localAbortReason == null ? 3 : 2)) {
            case 0 -> taskId = randomValueOtherThan(taskId, () -> randomAlphaOfLength(10));
            case 1 -> allocationId = randomValueOtherThan(allocationId, ESTestCase::randomNonNegativeLong);
            case 2 -> localAbortReason = localAbortReason == null ? randomAlphaOfLength(20) : null;
            case 3 -> exception = new ElasticsearchException(randomAlphaOfLength(10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Request(instance.masterNodeTimeout(), taskId, allocationId, exception, localAbortReason);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }
}
