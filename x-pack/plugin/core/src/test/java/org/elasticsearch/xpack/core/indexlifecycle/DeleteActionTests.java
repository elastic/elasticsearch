/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DeleteActionTests extends AbstractSerializingTestCase<DeleteAction> {

    @Override
    protected DeleteAction doParseInstance(XContentParser parser) throws IOException {
        return DeleteAction.parse(parser);
    }

    @Override
    protected DeleteAction createTestInstance() {
        return new DeleteAction();
    }

    @Override
    protected Reader<DeleteAction> instanceReader() {
        return DeleteAction::new;
    }
}
