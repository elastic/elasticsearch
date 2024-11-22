/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ReindexDataStreamPersistentTaskStateTests extends AbstractXContentSerializingTestCase<ReindexDataStreamPersistentTaskState> {
    @Override
    protected ReindexDataStreamPersistentTaskState doParseInstance(XContentParser parser) throws IOException {
        return ReindexDataStreamPersistentTaskState.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ReindexDataStreamPersistentTaskState> instanceReader() {
        return ReindexDataStreamPersistentTaskState::new;
    }

    @Override
    protected ReindexDataStreamPersistentTaskState createTestInstance() {
        return new ReindexDataStreamPersistentTaskState(randomNonNegativeLong());
    }

    @Override
    protected ReindexDataStreamPersistentTaskState mutateInstance(ReindexDataStreamPersistentTaskState instance) throws IOException {
        return new ReindexDataStreamPersistentTaskState(instance.completionTime() + 1);
    }
}
