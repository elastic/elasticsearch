/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification.Precision.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PrecisionResultTests extends AbstractWireSerializingTestCase<Result> {

    public static Result createRandom() {
        int numClasses = randomIntBetween(2, 100);
        List<String> classNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(numClasses).collect(Collectors.toList());
        List<PerClassSingleValue> classes = new ArrayList<>(numClasses);
        for (int i = 0; i < numClasses; i++) {
            double precision = randomDoubleBetween(0.0, 1.0, true);
            classes.add(new PerClassSingleValue(classNames.get(i), precision));
        }
        double avgPrecision = randomDoubleBetween(0.0, 1.0, true);
        return new Result(classes, avgPrecision);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(MlEvaluationNamedXContentProvider.getNamedWriteables());
    }

    @Override
    protected Result createTestInstance() {
        return createRandom();
    }

    @Override
    protected Result mutateInstance(Result instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Result> instanceReader() {
        return Result::new;
    }
}
