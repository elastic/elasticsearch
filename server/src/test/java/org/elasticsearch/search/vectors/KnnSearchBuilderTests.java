/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class KnnSearchBuilderTests extends AbstractSerializingTestCase<KnnSearchBuilder> {

    @Override
    protected KnnSearchBuilder doParseInstance(XContentParser parser) throws IOException {
        return KnnSearchBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<KnnSearchBuilder> instanceReader() {
        return KnnSearchBuilder::new;
    }

    @Override
    protected KnnSearchBuilder createTestInstance() {
        String field = randomAlphaOfLength(6);
        int dim = randomIntBetween(2, 30);
        float[] vector = randomVector(dim);
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k, 1000);

        KnnSearchBuilder builder = new KnnSearchBuilder(field, vector, k, numCands);
        if (randomBoolean()) {
            builder.boost(randomFloat());
        }
        return builder;
    }

    @Override
    protected KnnSearchBuilder mutateInstance(KnnSearchBuilder instance) throws IOException {
        switch (random().nextInt(5)) {
            case 0:
                String newField = randomValueOtherThan(instance.field, () -> randomAlphaOfLength(5));
                return new KnnSearchBuilder(newField, instance.queryVector, instance.k, instance.numCands + 3).boost(instance.boost);
            case 1:
                float[] newVector = randomValueOtherThan(instance.queryVector, () -> randomVector(5));
                return new KnnSearchBuilder(instance.field, newVector, instance.k + 3, instance.numCands).boost(instance.boost);
            case 2:
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k + 3, instance.numCands).boost(instance.boost);
            case 3:
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k, instance.numCands + 3).boost(instance.boost);
            case 4:
                float newBoost = randomValueOtherThan(instance.boost, ESTestCase::randomFloat);
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k, instance.numCands + 3).boost(newBoost);
            default:
                throw new IllegalStateException();
        }
    }

    public void testToQueryBuilder() {
        String field = randomAlphaOfLength(6);
        float[] vector = randomVector(randomIntBetween(2, 30));
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k, 1000);
        KnnSearchBuilder builder = new KnnSearchBuilder(field, vector, k, numCands);

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        if (randomBoolean()) {
            boost = randomFloat();
            builder.boost(boost);
        }

        KnnVectorQueryBuilder query = builder.toQueryBuilder();
        QueryBuilder expected = new KnnVectorQueryBuilder(field, vector, numCands).boost(boost);
        assertEquals(expected, query);
    }

    public void testNumCandsLessThanK() {
        KnnSearchBuilder builder = new KnnSearchBuilder("field", randomVector(3), 50, 10);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::toQueryBuilder);
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    public void testNumCandsExceedsLimit() {
        KnnSearchBuilder builder = new KnnSearchBuilder("field", randomVector(3), 100, 10002);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::toQueryBuilder);
        assertThat(e.getMessage(), containsString("[num_candidates] cannot exceed [10000]"));
    }

    public void testInvalidK() {
        KnnSearchBuilder builder = new KnnSearchBuilder("field", randomVector(3), 0, 100);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::toQueryBuilder);
        assertThat(e.getMessage(), containsString("[k] must be greater than 0"));
    }

    private static float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
