/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.usage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SemanticTextStatsTests extends AbstractBWCWireSerializationTestCase<SemanticTextStats> {

    @Override
    protected Writeable.Reader<SemanticTextStats> instanceReader() {
        return SemanticTextStats::new;
    }

    @Override
    protected SemanticTextStats createTestInstance() {
        return createRandomInstance();
    }

    static SemanticTextStats createRandomInstance() {
        return new SemanticTextStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected SemanticTextStats mutateInstance(SemanticTextStats instance) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> new SemanticTextStats(
                randomValueOtherThan(instance.getFieldCount(), ESTestCase::randomNonNegativeLong),
                instance.getIndicesCount(),
                instance.getInferenceIdCount()
            );
            case 1 -> new SemanticTextStats(
                instance.getFieldCount(),
                randomValueOtherThan(instance.getIndicesCount(), ESTestCase::randomNonNegativeLong),
                instance.getInferenceIdCount()
            );
            case 2 -> new SemanticTextStats(
                instance.getFieldCount(),
                instance.getIndicesCount(),
                randomValueOtherThan(instance.getInferenceIdCount(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new IllegalArgumentException();
        };
    }

    public void testDefaultConstructor() {
        var stats = new SemanticTextStats();
        assertThat(stats.getFieldCount(), equalTo(0L));
        assertThat(stats.getIndicesCount(), equalTo(0L));
        assertThat(stats.getInferenceIdCount(), equalTo(0L));
    }

    public void testAddFieldCount() {
        var stats = new SemanticTextStats();
        stats.addFieldCount(10L);
        assertThat(stats.getFieldCount(), equalTo(10L));
        stats.addFieldCount(32L);
        assertThat(stats.getFieldCount(), equalTo(42L));
    }

    public void testIsEmpty() {
        assertThat(new SemanticTextStats().isEmpty(), is(true));
        assertThat(new SemanticTextStats(randomLongBetween(1, Long.MAX_VALUE), 0, 0).isEmpty(), is(false));
        assertThat(new SemanticTextStats(0, randomLongBetween(1, Long.MAX_VALUE), 0).isEmpty(), is(false));
        assertThat(new SemanticTextStats(0, 0, randomLongBetween(1, Long.MAX_VALUE)).isEmpty(), is(false));
    }

    @Override
    protected SemanticTextStats mutateInstanceForVersion(SemanticTextStats instance, TransportVersion version) {
        return instance;
    }
}
