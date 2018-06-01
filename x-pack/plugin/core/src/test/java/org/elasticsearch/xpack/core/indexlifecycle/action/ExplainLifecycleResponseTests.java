/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExplainLifecycleResponseTests extends AbstractStreamableTestCase<ExplainLifecycleAction.Response> {
    
    @Override
    protected Response createTestInstance() {
        List<IndexLifecycleExplainResponse> indexResponses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
        }
        return new Response(indexResponses);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, in -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected Response mutateInstance(Response response) {
        List<IndexLifecycleExplainResponse> indexResponses = new ArrayList<>(response.getIndexResponses());
        if (indexResponses.size() > 0) {
            if (randomBoolean()) {
                indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
            } else {
                indexResponses.remove(indexResponses.size() - 1);
            }
        } else {
            indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
        }
        return new Response(indexResponses);
    }
}
