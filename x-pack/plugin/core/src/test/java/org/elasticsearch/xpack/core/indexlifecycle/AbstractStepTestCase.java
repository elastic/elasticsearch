/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public abstract class AbstractStepTestCase<T extends Step> extends ESTestCase {

    protected abstract T createRandomInstance();
    protected abstract T mutateInstance(T instance);
    protected abstract T copyInstance(T instance);

    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(), this::copyInstance, this::mutateInstance);
    }
}
