/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class RemoteFetchSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RemoteFetchSourceExec> {
    public static RemoteFetchSourceExec randomRemoteFetchSourceExec() {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 4, false);
        return new RemoteFetchSourceExec(source, output);
    }

    @Override
    protected RemoteFetchSourceExec createTestInstance() {
        return randomRemoteFetchSourceExec();
    }

    @Override
    protected RemoteFetchSourceExec mutateInstance(RemoteFetchSourceExec instance) throws IOException {
        return new RemoteFetchSourceExec(
            instance.source(),
            randomValueOtherThan(instance.output(), () -> randomFieldAttributes(1, 4, false))
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
