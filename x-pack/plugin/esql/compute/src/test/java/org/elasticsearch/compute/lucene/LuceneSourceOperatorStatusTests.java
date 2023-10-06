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
        return new LuceneSourceOperator.Status(0, 0, 1, 5, 123, 99990, 8000);
    }

    public static String simpleToJson() {
        return """
            {"processed_slices":0,"slice_index":0,"total_slices":1,"pages_emitted":5,"slice_min":123,"slice_max":99990,"current":8000}""";
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
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt()
        );
    }

    @Override
    protected LuceneSourceOperator.Status mutateInstance(LuceneSourceOperator.Status instance) {
        int processedSlices = instance.processedSlices();
        int sliceIndex = instance.sliceIndex();
        int totalSlices = instance.totalSlices();
        int pagesEmitted = instance.pagesEmitted();
        int sliceMin = instance.sliceMin();
        int sliceMax = instance.sliceMax();
        int current = instance.current();
        switch (between(0, 6)) {
            case 0 -> processedSlices = randomValueOtherThan(processedSlices, ESTestCase::randomNonNegativeInt);
            case 1 -> sliceIndex = randomValueOtherThan(sliceIndex, ESTestCase::randomNonNegativeInt);
            case 2 -> totalSlices = randomValueOtherThan(totalSlices, ESTestCase::randomNonNegativeInt);
            case 3 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 4 -> sliceMin = randomValueOtherThan(sliceMin, ESTestCase::randomNonNegativeInt);
            case 5 -> sliceMax = randomValueOtherThan(sliceMax, ESTestCase::randomNonNegativeInt);
            case 6 -> current = randomValueOtherThan(current, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        ;
        return new LuceneSourceOperator.Status(processedSlices, sliceIndex, totalSlices, pagesEmitted, sliceMin, sliceMax, current);
    }
}
