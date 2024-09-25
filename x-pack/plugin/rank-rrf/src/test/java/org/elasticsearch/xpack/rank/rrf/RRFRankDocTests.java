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
        RRFRankDoc instance = new RRFRankDoc(randomNonNegativeInt(), randomBoolean() ? -1 : randomNonNegativeInt(), queryCount);
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
        RRFRankDoc mutated = new RRFRankDoc(instance.doc, instance.shardIndex, instance.positions.length);
        mutated.score = instance.score;
        mutated.rank = instance.rank;
        System.arraycopy(instance.positions, 0, mutated.positions, 0, instance.positions.length);
        System.arraycopy(instance.scores, 0, mutated.scores, 0, instance.positions.length);
        mutated.rank = mutated.rank == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
        if (rarely()) {
            int ri = randomInt(mutated.positions.length - 1);
            mutated.positions[ri] = mutated.positions[ri] == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
        }
        if (rarely()) {
            int ri = randomInt(mutated.positions.length - 1);
            mutated.scores[ri] = randomFloat();
        }
        if (rarely()) {
            mutated.doc = randomNonNegativeInt();
        }
        if (rarely()) {
            mutated.score = randomFloat();
        }
        if (frequently()) {
            mutated.shardIndex = mutated.shardIndex == -1 ? randomNonNegativeInt() : -1;
        }
        return mutated;
    }

    public void testExplain() {
        RRFRankDoc instance = createTestRRFRankDoc();
        assertEquals(instance.explain().toString(), instance.explain().toString());
    }
}
