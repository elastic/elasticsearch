/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;

public class HealthNodeSelectionTaskParamsTests extends AbstractNamedWriteableTestCase<HealthNodeSelectorTaskParams> {

    // NOTE: This test case does not currently implement mutateInstance, because all instances of the class
    // are equal and have the same hashcode (for now).

    @Override
    protected HealthNodeSelectorTaskParams createTestInstance() {
        return new HealthNodeSelectorTaskParams();
    }

    @Override
    protected HealthNodeSelectorTaskParams copyInstance(HealthNodeSelectorTaskParams instance, Version version) throws IOException {
        return new HealthNodeSelectorTaskParams();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(HealthNodeSelectorTaskExecutor.getNamedWriteables());
    }

    @Override
    protected Class<HealthNodeSelectorTaskParams> categoryClass() {
        return HealthNodeSelectorTaskParams.class;
    }
}
