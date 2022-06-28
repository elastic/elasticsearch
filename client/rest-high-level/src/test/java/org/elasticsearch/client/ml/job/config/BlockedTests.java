/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class BlockedTests extends AbstractXContentTestCase<Blocked> {

    public static Blocked createRandom() {
        Blocked.Reason reason = randomFrom(Blocked.Reason.values());
        TaskId taskId = (reason != Blocked.Reason.NONE && randomBoolean())
            ? new TaskId(randomAlphaOfLength(10) + ":" + randomNonNegativeLong())
            : null;
        return new Blocked(reason, taskId);
    }

    @Override
    protected Blocked createTestInstance() {
        return createRandom();
    }

    @Override
    protected Blocked doParseInstance(XContentParser parser) throws IOException {
        return Blocked.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
