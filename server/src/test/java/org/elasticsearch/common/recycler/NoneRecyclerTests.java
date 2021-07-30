/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

public class NoneRecyclerTests extends AbstractRecyclerTestCase {

    @Override
    protected Recycler<byte[]> newRecycler(int limit) {
        return Recyclers.none(RECYCLER_C);
    }

    @Override
    protected void assertRecycled(byte[] data) {
        // will never match
    }

    @Override
    protected void assertDead(byte[] data) {
        // will never match
    }

}
