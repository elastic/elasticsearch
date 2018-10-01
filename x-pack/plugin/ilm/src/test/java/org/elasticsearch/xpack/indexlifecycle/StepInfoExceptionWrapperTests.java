/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class StepInfoExceptionWrapperTests extends ESTestCase {

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), (original) -> {
            try {
                return copy(original);
            } catch (Exception e) {
                // This should never happen
                throw new RuntimeException(e);
            }
        }, (original) -> {
            try {
                return mutate(original);
            } catch (Exception e) {
                // This should never happen
                throw new RuntimeException(e);
            }
        });
    }

    private static StepInfoExceptionWrapper createTestInstance() {
        return new StepInfoExceptionWrapper(new Exception(randomAlphaOfLengthBetween(5,20)));
    }

    private static StepInfoExceptionWrapper copy(final StepInfoExceptionWrapper original) throws Exception {
        Throwable thing = original.getExceptionType().getConstructor(String.class).newInstance(original.getMessage());
        return new StepInfoExceptionWrapper(thing);
    }

    @SuppressWarnings("unchecked")
    private static StepInfoExceptionWrapper mutate(final StepInfoExceptionWrapper original) throws Exception {
        Class<? extends Throwable> exceptionType = randomValueOtherThan(original.getExceptionType(), () ->
            randomFrom(RuntimeException.class, IllegalArgumentException.class, Exception.class, IllegalStateException.class));
        final String message;
        if (randomBoolean()) {
            message = original.getMessage();
        } else {
            message = randomAlphaOfLengthBetween(5, 20);
        }
        return new StepInfoExceptionWrapper(exceptionType.getConstructor(String.class).newInstance(message));
    }

}
