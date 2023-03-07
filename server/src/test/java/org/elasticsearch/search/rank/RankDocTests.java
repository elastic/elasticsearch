/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;

public abstract class RankDocTests<RD extends RankDoc> extends AbstractNamedWriteableTestCase<RD> {

    @Override
    protected final RD createTestInstance() {
        RD rd = doCreateTestInstance();
        rd.doc = randomNonNegativeInt();
        rd.score = randomFloat();
        rd.shardIndex = randomBoolean() ? -1 : randomNonNegativeInt();
        return rd;
    }

    protected abstract RD doCreateTestInstance();

    @Override
    protected final RD mutateInstance(RD instance) throws IOException {
        RD rd = doMutateInstance(instance);
        if (rarely()) {
            rd.doc = randomNonNegativeInt();
        }
        if (rarely()) {
            rd.score = randomFloat();
        }
        rd.shardIndex = rd.shardIndex == -1 ? randomNonNegativeInt() : -1;
        return rd;
    }

    protected abstract RD doMutateInstance(RD instance) throws IOException;
}
