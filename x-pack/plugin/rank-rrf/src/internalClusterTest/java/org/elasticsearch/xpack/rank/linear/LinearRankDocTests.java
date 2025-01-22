/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.rank.AbstractRankDocWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LinearRankDocTests extends AbstractRankDocWireSerializingTestCase<LinearRankDoc> {

    protected LinearRankDoc createTestRankDoc() {
        int queries = randomIntBetween(2, 20);
        float[] weights = new float[queries];
        String[] normalizers = new String[queries];
        float[] normalizedScores = new float[queries];
        for (int i = 0; i < queries; i++) {
            weights[i] = randomFloat();
            normalizers[i] = randomAlphaOfLengthBetween(1, 10);
            normalizedScores[i] = randomFloat();
        }
        LinearRankDoc rankDoc = new LinearRankDoc(randomNonNegativeInt(), randomFloat(), randomIntBetween(0, 1), weights, normalizers);
        rankDoc.rank = randomNonNegativeInt();
        rankDoc.normalizedScores = normalizedScores;
        return rankDoc;
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getAdditionalNamedWriteables() {
        return Collections.emptyList();
    }

    @Override
    protected Writeable.Reader<LinearRankDoc> instanceReader() {
        return LinearRankDoc::new;
    }

    @Override
    protected LinearRankDoc mutateInstance(LinearRankDoc instance) throws IOException {
        LinearRankDoc mutated = new LinearRankDoc(
            instance.doc,
            instance.score,
            instance.shardIndex,
            instance.weights,
            instance.normalizers
        );
        mutated.normalizedScores = instance.normalizedScores;
        mutated.rank = instance.rank;
        if (frequently()) {
            mutated.doc = randomNonNegativeInt();
        }
        if (frequently()) {
            mutated.score = randomFloat();
        }
        if (frequently()) {
            mutated.shardIndex = randomNonNegativeInt();
        }
        if (frequently()) {
            mutated.rank = randomNonNegativeInt();
        }
        if (frequently()) {
            for (int i = 0; i < mutated.normalizedScores.length; i++) {
                if (frequently()) {
                    mutated.normalizedScores[i] = randomFloat();
                }
            }
        }
        if (frequently()) {
            for (int i = 0; i < mutated.weights.length; i++) {
                if (frequently()) {
                    mutated.weights[i] = randomFloat();
                }
            }
        }
        if (frequently()) {
            for (int i = 0; i < mutated.normalizers.length; i++) {
                if (frequently()) {
                    mutated.normalizers[i] = randomAlphaOfLengthBetween(1, 10);
                }
            }
        }
        return mutated;
    }
}
