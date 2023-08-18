/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;

public class SystemIndexMigrationTaskParamsTests extends AbstractNamedWriteableTestCase<SystemIndexMigrationTaskParams> {

    // NOTE: This test case does not currently implement mutateInstance, because all instances of the class
    // are equal and have the same hashcode (for now).

    @Override
    protected SystemIndexMigrationTaskParams createTestInstance() {
        return new SystemIndexMigrationTaskParams();
    }

    @Override
    protected SystemIndexMigrationTaskParams mutateInstance(SystemIndexMigrationTaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected SystemIndexMigrationTaskParams copyInstance(SystemIndexMigrationTaskParams instance, TransportVersion version)
        throws IOException {
        return new SystemIndexMigrationTaskParams();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SystemIndexMigrationExecutor.getNamedWriteables());
    }

    @Override
    protected Class<SystemIndexMigrationTaskParams> categoryClass() {
        return SystemIndexMigrationTaskParams.class;
    }
}
