/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class BaseVectorizationTests extends ESTestCase {

    @Before
    public void sanity() {
        assert Runtime.version().feature() < 21 || ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    }

    public static ESVectorizationProvider defaultProvider() {
        return new DefaultESVectorizationProvider();
    }

    public static ESVectorizationProvider maybePanamaProvider() {
        return ESVectorizationProvider.lookup(true);
    }
}
