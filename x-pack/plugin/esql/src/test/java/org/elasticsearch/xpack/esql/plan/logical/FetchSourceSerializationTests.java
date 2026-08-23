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

public class FetchSourceSerializationTests extends AbstractLogicalPlanSerializationTests<FetchSource> {
    public static FetchSource randomFetchSource() {
        List<Attribute> output = randomFieldAttributes(1, 4, false);
        return new FetchSource(randomSource(), output);
    }

    @Override
    protected FetchSource createTestInstance() {
        return randomFetchSource();
    }

    @Override
    protected FetchSource mutateInstance(FetchSource instance) throws IOException {
        return new FetchSource(instance.source(), randomValueOtherThan(instance.output(), () -> randomFieldAttributes(1, 4, false)));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
