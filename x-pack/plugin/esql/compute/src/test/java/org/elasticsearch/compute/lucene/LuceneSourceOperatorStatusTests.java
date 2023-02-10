/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LuceneSourceOperatorStatusTests extends AbstractWireSerializingTestCase<LuceneSourceOperator.Status> {
    public static LuceneSourceOperator.Status simple() {
        return new LuceneSourceOperator.Status(0, 1, 5, 123, 99990);
    }

    public static String simpleToJson() {
        return """
            {"current_leaf":0,"total_leaves":1,"leaf_position":123,"leaf_size":99990,"pages_emitted":5}""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple()), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<LuceneSourceOperator.Status> instanceReader() {
        return LuceneSourceOperator.Status::new;
    }

    @Override
    public LuceneSourceOperator.Status createTestInstance() {
        return new LuceneSourceOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt()
        );
    }

    @Override
    protected LuceneSourceOperator.Status mutateInstance(LuceneSourceOperator.Status instance) {
        return switch (between(0, 4)) {
            case 0 -> new LuceneSourceOperator.Status(
                randomValueOtherThan(instance.currentLeaf(), ESTestCase::randomNonNegativeInt),
                instance.totalLeaves(),
                instance.pagesEmitted(),
                instance.leafPosition(),
                instance.leafSize()
            );
            case 1 -> new LuceneSourceOperator.Status(
                instance.currentLeaf(),
                randomValueOtherThan(instance.totalLeaves(), ESTestCase::randomNonNegativeInt),
                instance.pagesEmitted(),
                instance.leafPosition(),
                instance.leafSize()
            );
            case 2 -> new LuceneSourceOperator.Status(
                instance.currentLeaf(),
                instance.totalLeaves(),
                randomValueOtherThan(instance.pagesEmitted(), ESTestCase::randomNonNegativeInt),
                instance.leafPosition(),
                instance.leafSize()
            );
            case 3 -> new LuceneSourceOperator.Status(
                instance.currentLeaf(),
                instance.totalLeaves(),
                instance.pagesEmitted(),
                randomValueOtherThan(instance.leafPosition(), ESTestCase::randomNonNegativeInt),
                instance.leafSize()
            );
            case 4 -> new LuceneSourceOperator.Status(
                instance.currentLeaf(),
                instance.totalLeaves(),
                instance.pagesEmitted(),
                instance.leafPosition(),
                randomValueOtherThan(instance.leafSize(), ESTestCase::randomNonNegativeInt)
            );
            default -> throw new UnsupportedOperationException();
        };
    }
}
