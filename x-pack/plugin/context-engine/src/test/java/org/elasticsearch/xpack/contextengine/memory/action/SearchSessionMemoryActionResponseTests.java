/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.contextengine.memory.SessionMemoryTestUtils;

import java.util.List;

public class SearchSessionMemoryActionResponseTests extends AbstractWireSerializingTestCase<SearchSessionMemoryAction.Response> {

    @Override
    protected Writeable.Reader<SearchSessionMemoryAction.Response> instanceReader() {
        return SearchSessionMemoryAction.Response::new;
    }

    @Override
    protected SearchSessionMemoryAction.Response createTestInstance() {
        return new SearchSessionMemoryAction.Response(randomHits());
    }

    @Override
    protected SearchSessionMemoryAction.Response mutateInstance(SearchSessionMemoryAction.Response instance) {
        return new SearchSessionMemoryAction.Response(
            randomValueOtherThan(instance.hits(), SearchSessionMemoryActionResponseTests::randomHits)
        );
    }

    private static List<SearchSessionMemoryAction.Hit> randomHits() {
        return randomList(0, 5, () -> new SearchSessionMemoryAction.Hit(SessionMemoryTestUtils.randomSessionMemory(), randomFloat()));
    }
}
