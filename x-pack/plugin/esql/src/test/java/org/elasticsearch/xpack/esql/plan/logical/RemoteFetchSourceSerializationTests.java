/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

public class RemoteFetchSourceSerializationTests extends AbstractLogicalPlanSerializationTests<RemoteFetchSource> {
    public static RemoteFetchSource randomRemoteFetchSource() {
        List<Attribute> output = randomFieldAttributes(1, 4, false);
        return new RemoteFetchSource(randomSource(), output);
    }

    @Override
    protected RemoteFetchSource createTestInstance() {
        return randomRemoteFetchSource();
    }

    @Override
    protected RemoteFetchSource mutateInstance(RemoteFetchSource instance) throws IOException {
        return new RemoteFetchSource(instance.source(), randomValueOtherThan(instance.output(), () -> randomFieldAttributes(1, 4, false)));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
