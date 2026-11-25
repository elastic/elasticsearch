/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.resumeinfo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

/**
 * Wire serialization tests for {@link ResumeInfo.RelocationOrigin}.
 */
public class RelocationOriginWireSerializingTests extends AbstractWireSerializingTestCase<ResumeInfo.RelocationOrigin> {

    @Override
    protected Writeable.Reader<ResumeInfo.RelocationOrigin> instanceReader() {
        return ResumeInfo.RelocationOrigin::new;
    }

    @Override
    protected ResumeInfo.RelocationOrigin createTestInstance() {
        return new ResumeInfo.RelocationOrigin(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLengthBetween(1, 10), randomNonNegativeLong()),
            randomLong()
        );
    }

    @Override
    protected ResumeInfo.RelocationOrigin mutateInstance(ResumeInfo.RelocationOrigin instance) throws IOException {
        // RelocationOrigin is a record so instance based equality doesn't need to be tested
        return null;
    }
}
