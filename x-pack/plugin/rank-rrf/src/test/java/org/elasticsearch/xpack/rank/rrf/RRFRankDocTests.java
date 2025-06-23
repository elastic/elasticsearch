/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.rank.AbstractRankDocWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.rank.rrf.RRFRankDoc.NO_RANK;

public class RRFRankDocTests extends AbstractRankDocWireSerializingTestCase<RRFRankDoc> {

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

    @Override
    protected List<NamedWriteableRegistry.Entry> getAdditionalNamedWriteables() {
        try (RRFRankPlugin rrfRankPlugin = new RRFRankPlugin()) {
            return rrfRankPlugin.getNamedWriteables();
        } catch (IOException ex) {
            throw new AssertionError("Failed to create RRFRankPlugin", ex);
        }
    }

    @Override
    protected Reader<RRFRankDoc> instanceReader() {
        return RRFRankDoc::new;
    }

    @Override
    protected RRFRankDoc createTestRankDoc() {
        int queryCount = randomIntBetween(2, 20);
        return createTestRRFRankDoc(queryCount);
    }

    @Override
    protected RRFRankDoc mutateInstance(RRFRankDoc instance) throws IOException {
        int doc = instance.doc;
        int shardIndex = instance.shardIndex;
        float score = instance.score;
        int rankConstant = instance.rankConstant;
        int rank = instance.rank;
        int queries = instance.positions.length;
        int[] positions = new int[queries];
        float[] scores = new float[queries];

        switch (randomInt(6)) {
            case 0:
                doc = randomValueOtherThan(doc, ESTestCase::randomNonNegativeInt);
                break;
            case 1:
                shardIndex = shardIndex == -1 ? randomNonNegativeInt() : -1;
                break;
            case 2:
                score = randomValueOtherThan(score, ESTestCase::randomFloat);
                break;
            case 3:
                rankConstant = randomValueOtherThan(rankConstant, () -> randomIntBetween(1, 100));
                break;
            case 4:
                rank = rank == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
                break;
            case 5:
                for (int i = 0; i < queries; i++) {
                    positions[i] = instance.positions[i] == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
                }
                break;
            case 6:
                for (int i = 0; i < queries; i++) {
                    scores[i] = randomValueOtherThan(scores[i], ESTestCase::randomFloat);
                }
                break;
            default:
                throw new AssertionError();
        }
        RRFRankDoc mutated = new RRFRankDoc(doc, shardIndex, queries, rankConstant);
        System.arraycopy(positions, 0, mutated.positions, 0, instance.positions.length);
        System.arraycopy(scores, 0, mutated.scores, 0, instance.scores.length);
        mutated.rank = rank;
        mutated.score = score;
        return mutated;
    }
}
