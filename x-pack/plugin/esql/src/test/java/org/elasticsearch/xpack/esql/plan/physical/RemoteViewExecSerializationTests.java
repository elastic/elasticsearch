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

public class RemoteViewExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RemoteViewExec> {
    static RemoteViewExec randomRemoteViewExec() {
        Source source = randomSource();
        String viewName = randomAlphaOfLength(8);
        String handle = randomAlphaOfLength(6);
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        return new RemoteViewExec(source, viewName, handle, output);
    }

    @Override
    protected RemoteViewExec createTestInstance() {
        return randomRemoteViewExec();
    }

    @Override
    protected RemoteViewExec mutateInstance(RemoteViewExec instance) throws IOException {
        String viewName = instance.viewName();
        String handle = instance.handle();
        List<Attribute> output = instance.output();
        switch (between(0, 2)) {
            case 0 -> viewName = randomValueOtherThan(viewName, () -> randomAlphaOfLength(8));
            case 1 -> handle = randomValueOtherThan(handle, () -> randomAlphaOfLength(6));
            case 2 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
        }
        return new RemoteViewExec(instance.source(), viewName, handle, output);
    }
}
