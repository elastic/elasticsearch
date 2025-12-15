/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public class TransportReadinessActionTests extends ESTestCase {
    public void testReady() throws Exception {
        assertReadinessEquals(true, () -> true);
    }

    public void testNotReady() throws Exception {
        assertReadinessEquals(false, () -> false);
    }

    private void assertReadinessEquals(boolean expected, BooleanSupplier readinessSupplier) throws Exception {
        TransportReadinessAction action = new TransportReadinessAction(
            new ActionFilters(Set.of()),
            null,
            Runnable::run,  // Just run synchronously
            readinessSupplier
        );

        AtomicReference<ReadinessResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        action.doExecute(null, new ReadinessRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ReadinessResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        });

        assertNull("Should not fail", failureRef.get());
        assertEquals(new ReadinessResponse(expected), responseRef.get());
    }

}
