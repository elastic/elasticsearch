/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.rank.rrf.RRFRankDoc.NO_RANK;

public class RRFRankDocTests extends AbstractWireSerializingTestCase<RRFRankDoc> {

    static RRFRankDoc createTestRRFRankDoc(int queryCount) {
        RRFRankDoc instance = new RRFRankDoc(
            randomNonNegativeInt(),
            randomBoolean() ? -1 : randomNonNegativeInt(),
            queryCount,
            randomIntBetween(1, 100)
        );
        instance.score = randomFloat();
        instance.rank = randomBoolean() ? NO_RANK : randomIntBetween(1, 10000);
        for (int qi = 0; qi < queryCount; ++qi) {
            if (randomBoolean()) {
                instance.positions[qi] = randomIntBetween(1, 10000);
                instance.scores[qi] = randomFloat();
            }
        }
        return instance;
    }

    static RRFRankDoc createTestRRFRankDoc() {
        int queryCount = randomIntBetween(2, 20);
        return createTestRRFRankDoc(queryCount);
    }

    @Override
    protected Reader<RRFRankDoc> instanceReader() {
        return RRFRankDoc::new;
    }

    @Override
    protected RRFRankDoc createTestInstance() {
        return createTestRRFRankDoc();
    }

    @Override
    protected RRFRankDoc mutateInstance(RRFRankDoc instance) throws IOException {
        int doc = instance.doc;
        if (rarely()) {
            doc = randomNonNegativeInt();
        }
        int shardIndex = instance.shardIndex;
        if (frequently()) {
            shardIndex = shardIndex == -1 ? randomNonNegativeInt() : -1;
        }
        float score = instance.score;
        if (frequently()) {
            score = randomFloat();
        }
        int rankConstant = instance.rankConstant;
        if (frequently()) {
            rankConstant = randomIntBetween(1, 100);
        }
        int rank = instance.rank;
        if (frequently()) {
            rank = rank == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
        }
        int queries = instance.positions.length;
        int[] positions = new int[queries];
        float[] scores = new float[queries];
        for (int i = 0; i < queries; i++) {
            if (rarely()) {
                positions[i] = instance.positions[i] == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
            } else {
                positions[i] = instance.positions[i];
            }
            if (rarely()) {
                scores[i] = randomFloat();
            } else {
                scores[i] = instance.scores[i];
            }
        }
        RRFRankDoc mutated = new RRFRankDoc(doc, shardIndex, queries, rankConstant);
        System.arraycopy(positions, 0, mutated.positions, 0, instance.positions.length);
        System.arraycopy(scores, 0, mutated.scores, 0, instance.scores.length);
        mutated.rank = rank;
        mutated.score = score;
        return mutated;
    }

    public void testExplain() {
        RRFRankDoc instance = createTestRRFRankDoc();
        assertEquals(instance.explain().toString(), instance.explain().toString());
    }
}
