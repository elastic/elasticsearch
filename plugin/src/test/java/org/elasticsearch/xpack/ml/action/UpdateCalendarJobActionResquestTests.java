/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

import java.util.HashSet;
import java.util.Set;

public class UpdateCalendarJobActionResquestTests extends AbstractStreamableTestCase<UpdateCalendarJobAction.Request> {

    @Override
    protected UpdateCalendarJobAction.Request createTestInstance() {
        int addSize = randomIntBetween(0, 2);
        Set<String> toAdd = new HashSet<>();
        for (int i=0; i<addSize; i++) {
            toAdd.add(randomAlphaOfLength(10));
        }

        int removeSize = randomIntBetween(0, 2);
        Set<String> toRemove = new HashSet<>();
        for (int i=0; i<removeSize; i++) {
            toRemove.add(randomAlphaOfLength(10));
        }

        return new UpdateCalendarJobAction.Request(randomAlphaOfLength(10), toAdd, toRemove);
    }

    @Override
    protected UpdateCalendarJobAction.Request  createBlankInstance() {
        return new UpdateCalendarJobAction.Request();
    }
}
