/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.List;

public class RRFRankShardResultTests extends AbstractNamedWriteableTestCase<RRFRankShardResult> {

    @Override
    protected RRFRankShardResult createTestInstance() {
        int queryCount = randomIntBetween(2, 20);
        int docCount = frequently() ? randomIntBetween(1, 10000) : 0;
        RRFRankDoc[] docs = new RRFRankDoc[docCount];
        for (int di = 0; di < docCount; ++di) {
            docs[di] = RRFRankDocTests.createTestRRFRankDoc(queryCount);
        }
        return new RRFRankShardResult(queryCount, docs);
    }

    @Override
    protected RRFRankShardResult mutateInstance(RRFRankShardResult instance) throws IOException {
        if (instance.rrfRankDocs.length == 0) {
            return new RRFRankShardResult(
                instance.queryCount,
                new RRFRankDoc[] { RRFRankDocTests.createTestRRFRankDoc(instance.queryCount) }
            );
        }
        RRFRankDoc[] docs = new RRFRankDoc[instance.rrfRankDocs.length];
        System.arraycopy(instance.rrfRankDocs, 0, docs, 0, instance.rrfRankDocs.length);
        docs[randomInt(instance.rrfRankDocs.length - 1)] = RRFRankDocTests.createTestRRFRankDoc(instance.queryCount);
        return new RRFRankShardResult(instance.queryCount, docs);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(RRFRankShardResult.class, RRFRankPlugin.NAME, RRFRankShardResult::new))
        );
    }

    @Override
    protected Class<RRFRankShardResult> categoryClass() {
        return RRFRankShardResult.class;
    }
}
