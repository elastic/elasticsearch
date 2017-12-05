/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ReplicasActionTests extends AbstractSerializingTestCase<ReplicasAction> {

    @Override
    protected ReplicasAction doParseInstance(XContentParser parser) throws IOException {
        return ReplicasAction.parse(parser);
    }

    @Override
    protected ReplicasAction createTestInstance() {
        return new ReplicasAction();
    }

    @Override
    protected Reader<ReplicasAction> instanceReader() {
        return ReplicasAction::new;
    }
}
