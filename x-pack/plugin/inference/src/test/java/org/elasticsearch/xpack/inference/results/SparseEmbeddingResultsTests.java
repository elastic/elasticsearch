/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparseEmbeddingResultsTests extends AbstractWireSerializingTestCase<SparseEmbeddingResults> {

    public static SparseEmbeddingResults createRandomResults() {
        int numEmbeddings = randomIntBetween(1, 10);
        int numTokens = randomIntBetween(0, 20);
        return createRandomResults(numEmbeddings, numTokens);
    }

    public static SparseEmbeddingResults createRandomResults(int numEmbeddings, int numTokens) {
        List<SparseEmbeddingResults.Embedding> embeddings = new ArrayList<>(numEmbeddings);

        for (int i = 0; i < numEmbeddings; i++) {
            embeddings.add(createRandomEmbedding(numTokens));
        }

        return new SparseEmbeddingResults(embeddings, randomBoolean());
    }

    public static SparseEmbeddingResults createRandomResults(List<String> input) {
        List<SparseEmbeddingResults.Embedding> embeddings = new ArrayList<>(input.size());

        for (int i = 0; i < input.size(); i++) {
            int numTokens = Strings.tokenizeToStringArray(input.get(i), " ").length;
            embeddings.add(createRandomEmbedding(numTokens));
        }

        return new SparseEmbeddingResults(embeddings, randomBoolean());
    }

    private static SparseEmbeddingResults.Embedding createRandomEmbedding(int numTokens) {
        List<SparseEmbeddingResults.WeightedToken> tokenList = new ArrayList<>(numTokens);
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new SparseEmbeddingResults.WeightedToken(Integer.toString(i), (float) randomDoubleBetween(0.0, 5.0, false)));
        }

        return new SparseEmbeddingResults.Embedding(tokenList);
    }

    @Override
    protected Writeable.Reader<SparseEmbeddingResults> instanceReader() {
        return SparseEmbeddingResults::new;
    }

    @Override
    protected SparseEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected SparseEmbeddingResults mutateInstance(SparseEmbeddingResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new SparseEmbeddingResults(instance.embeddings().subList(0, end), instance.isTruncated());
        } else {
            List<SparseEmbeddingResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding(randomIntBetween(0, 20)));
            return new SparseEmbeddingResults(embeddings, instance.isTruncated());
        }
    }
}
