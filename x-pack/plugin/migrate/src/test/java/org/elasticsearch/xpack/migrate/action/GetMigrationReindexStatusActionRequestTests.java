/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Request;

import java.io.IOException;

public class GetMigrationReindexStatusActionRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(100));
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        return createTestInstance(); // There's only one field
    }
}
