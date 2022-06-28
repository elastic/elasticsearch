/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

public class ConcurrentRecyclerTests extends AbstractRecyclerTestCase {

    @Override
    protected Recycler<byte[]> newRecycler(int limit) {
        return Recyclers.concurrent(Recyclers.dequeFactory(RECYCLER_C, limit), randomIntBetween(1, 5));
    }

}
