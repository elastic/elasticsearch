/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.transform.IntRange;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformIndex;

public final class TransformTestUtils {
    private TransformTestUtils() {
    }

    public static List<TransformIndex> createIndexArray(TransformIndex... indexs) {
        List<TransformIndex> result = new ArrayList<Transform.TransformIndex>();
        for (TransformIndex i : indexs) {
            result.add(i);
        }

        return result;
    }

    public static TransformConfig createValidTransform(TransformType type) {
        List<String> inputs = createValidArgs(type.arityRange(), type,
                (arg, t) -> Integer.toString(arg));
        List<String> args = createValidArgs(type.argumentsRange(), type,
                TransformTestUtils::createValidArgument);
        List<String> outputs = createValidArgs(type.outputsRange(), type,
                (arg, t) -> Integer.toString(arg));

        Condition condition = null;
        if (type.hasCondition()) {
            condition = new Condition(Operator.EQ, "100");
        }

        TransformConfig tr = new TransformConfig(type.toString());
        tr.setInputs(inputs);
        tr.setArguments(args);
        tr.setOutputs(outputs);
        tr.setCondition(condition);
        return tr;
    }

    private static List<String> createValidArgs(IntRange range, TransformType type,
            BiFunction<Integer, TransformType, String> argumentCreator) {
        List<String> args = new ArrayList<>();
        int validCount = getValidCount(range);
        for (int arg = 0; arg < validCount; ++arg) {
            args.add(argumentCreator.apply(arg, type));
        }
        return args;
    }

    private static String createValidArgument(int argNumber, TransformType type) {
        switch (type) {
        case REGEX_EXTRACT:
            return Integer.toString(argNumber) + ".Foo ([0-9]+)";
        case CONCAT:
        case DOMAIN_SPLIT:
        case EXCLUDE:
        case LOWERCASE:
        case REGEX_SPLIT:
        case TRIM:
        case UPPERCASE:
            return Integer.toString(argNumber);
        default:
            throw new IllegalArgumentException();
        }
    }

    private static int getValidCount(IntRange range) {
        return range.hasUpperBound() ? range.upper() : range.lower();
    }
}
