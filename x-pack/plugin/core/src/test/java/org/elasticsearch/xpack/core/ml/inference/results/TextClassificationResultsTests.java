/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

public class TextClassificationResultsTests extends AbstractWireSerializingTestCase<TextClassificationResults> {
    @Override
    protected Writeable.Reader<TextClassificationResults> instanceReader() {
        return TextClassificationResults::new;
    }

    @Override
    protected TextClassificationResults createTestInstance() {
        return new TextClassificationResults(
            Stream.generate(TopClassEntryTests::createRandomTopClassEntry).limit(randomIntBetween(2, 5)).collect(Collectors.toList())
        );
    }

    public void testAsMap() {
        TextClassificationResults testInstance = new TextClassificationResults(
            List.of(
                new TopClassEntry("foo", 1.0),
                new TopClassEntry("bar", 0.0)
            )
        );
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.keySet(), hasSize(2));
        assertThat(1.0, closeTo((Double)asMap.get("foo"), 0.0001));
        assertThat(0.0, closeTo((Double)asMap.get("bar"), 0.0001));
    }
}
