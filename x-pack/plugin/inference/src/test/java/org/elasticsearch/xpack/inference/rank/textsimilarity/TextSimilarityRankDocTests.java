/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.rank.AbstractRankDocWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.rank.RankDoc.NO_RANK;

public class TextSimilarityRankDocTests extends AbstractRankDocWireSerializingTestCase<TextSimilarityRankDoc> {

    static TextSimilarityRankDoc createTestTextSimilarityRankDoc() {
        TextSimilarityRankDoc instance = new TextSimilarityRankDoc(
            randomNonNegativeInt(),
            randomFloat(),
            randomBoolean() ? -1 : randomNonNegativeInt(),
            randomAlphaOfLength(randomIntBetween(2, 5)),
            randomAlphaOfLength(randomIntBetween(2, 5))
        );
        instance.rank = randomBoolean() ? NO_RANK : randomIntBetween(1, 10000);
        return instance;
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getAdditionalNamedWriteables() {
        try (InferencePlugin plugin = new InferencePlugin(Settings.EMPTY)) {
            return plugin.getNamedWriteables();
        }
    }

    @Override
    protected Writeable.Reader<TextSimilarityRankDoc> instanceReader() {
        return TextSimilarityRankDoc::new;
    }

    @Override
    protected TextSimilarityRankDoc createTestRankDoc() {
        return createTestTextSimilarityRankDoc();
    }

    @Override
    protected TextSimilarityRankDoc mutateInstance(TextSimilarityRankDoc instance) throws IOException {
        int doc = instance.doc;
        int shardIndex = instance.shardIndex;
        float score = instance.score;
        int rank = instance.rank;
        String inferenceId = instance.inferenceId;
        String field = instance.field;

        switch (randomInt(5)) {
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
                rank = rank == NO_RANK ? randomIntBetween(1, 10000) : NO_RANK;
                break;
            case 4:
                inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(randomIntBetween(2, 5)));
                break;
            case 5:
                field = randomValueOtherThan(field, () -> randomAlphaOfLength(randomIntBetween(2, 5)));
                break;
            default:
                throw new AssertionError();
        }
        TextSimilarityRankDoc mutated = new TextSimilarityRankDoc(doc, score, shardIndex, inferenceId, field);
        mutated.rank = rank;
        return mutated;
    }
}
