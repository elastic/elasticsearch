/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class MigrateToDataTiersRequestTests extends AbstractWireSerializingTestCase<MigrateToDataTiersRequest> {

    @Override
    protected Writeable.Reader<MigrateToDataTiersRequest> instanceReader() {
        return MigrateToDataTiersRequest::new;
    }

    @Override
    protected MigrateToDataTiersRequest createTestInstance() {
        return new MigrateToDataTiersRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected MigrateToDataTiersRequest mutateInstance(MigrateToDataTiersRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
