/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MulticlassConfusionMatrixResultTests extends AbstractSerializingTestCase<MulticlassConfusionMatrix.Result> {

    @Override
    protected MulticlassConfusionMatrix.Result doParseInstance(XContentParser parser) throws IOException {
        return MulticlassConfusionMatrix.Result.fromXContent(parser);
    }

    @Override
    protected MulticlassConfusionMatrix.Result createTestInstance() {
        int numClasses = randomIntBetween(2, 100);
        List<String> classNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(numClasses).collect(Collectors.toList());
        Map<String, Map<String, Long>> confusionMatrix = new TreeMap<>();
        for (int i = 0; i < numClasses; i++) {
            Map<String, Long> row = new TreeMap<>();
            confusionMatrix.put(classNames.get(i), row);
            for (int j = 0; j < numClasses; j++) {
                if (randomBoolean()) {
                    row.put(classNames.get(i), randomNonNegativeLong());
                }
            }
        }
        long otherClassesCount = randomNonNegativeLong();
        return new MulticlassConfusionMatrix.Result(confusionMatrix, otherClassesCount);
    }

    @Override
    protected Writeable.Reader<MulticlassConfusionMatrix.Result> instanceReader() {
        return MulticlassConfusionMatrix.Result::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> !field.isEmpty();
    }
}
