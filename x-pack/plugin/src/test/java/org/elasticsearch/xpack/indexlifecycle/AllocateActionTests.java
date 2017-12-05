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

public class AllocateActionTests extends AbstractSerializingTestCase<AllocateAction> {

    @Override
    protected AllocateAction doParseInstance(XContentParser parser) throws IOException {
        return AllocateAction.parse(parser);
    }

    @Override
    protected AllocateAction createTestInstance() {
        return new AllocateAction();
    }

    @Override
    protected Reader<AllocateAction> instanceReader() {
        return AllocateAction::new;
    }
}
