/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.ActualClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.PredictedClass;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.MulticlassConfusionMatrix.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MulticlassConfusionMatrixResultTests extends AbstractSerializingTestCase<Result> {

    public static Result createRandom() {
        int numClasses = randomIntBetween(2, 100);
        List<String> classNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(numClasses).collect(Collectors.toList());
        List<ActualClass> actualClasses = new ArrayList<>(numClasses);
        for (int i = 0; i < numClasses; i++) {
            List<PredictedClass> predictedClasses = new ArrayList<>(numClasses);
            for (int j = 0; j < numClasses; j++) {
                predictedClasses.add(new PredictedClass(classNames.get(j), randomNonNegativeLong()));
            }
            actualClasses.add(new ActualClass(classNames.get(i), randomNonNegativeLong(), predictedClasses, randomNonNegativeLong()));
        }
        return new Result(actualClasses, randomNonNegativeLong());
    }

    @Override
    protected Result doParseInstance(XContentParser parser) throws IOException {
        return Result.fromXContent(parser);
    }

    @Override
    protected Result createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Result> instanceReader() {
        return Result::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }

    public void testConstructor_ValidationFailures() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Result(null, 0));
            assertThat(e.getMessage(), equalTo("[confusion_matrix] must not be null."));
        }
        {
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> new Result(Collections.emptyList(), -1));
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), equalTo("[other_actual_class_count] must be >= 0, was: -1"));
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new Result(Collections.singletonList(new ActualClass(null, 0, Collections.emptyList(), 0)), 0)
            );
            assertThat(e.getMessage(), equalTo("[actual_class] must not be null."));
        }
        {
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> new Result(Collections.singletonList(new ActualClass("actual_class", -1, Collections.emptyList(), 0)), 0)
            );
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), equalTo("[actual_class_doc_count] must be >= 0, was: -1"));
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new Result(Collections.singletonList(new ActualClass("actual_class", 0, null, 0)), 0)
            );
            assertThat(e.getMessage(), equalTo("[predicted_classes] must not be null."));
        }
        {
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> new Result(Collections.singletonList(new ActualClass("actual_class", 0, Collections.emptyList(), -1)), 0)
            );
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), equalTo("[other_predicted_class_doc_count] must be >= 0, was: -1"));
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new Result(
                    Collections.singletonList(
                        new ActualClass("actual_class", 0, Collections.singletonList(new PredictedClass(null, 0)), 0)
                    ),
                    0
                )
            );
            assertThat(e.getMessage(), equalTo("[predicted_class] must not be null."));
        }
        {
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> new Result(
                    Collections.singletonList(
                        new ActualClass("actual_class", 0, Collections.singletonList(new PredictedClass("predicted_class", -1)), 0)
                    ),
                    0
                )
            );
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), equalTo("[count] must be >= 0, was: -1"));
        }
    }
}
