/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform.verification;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.config.transform.IntRange;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;

import java.util.List;

public final class TransformConfigVerifier {
    private TransformConfigVerifier() {
        // Hide default constructor
    }

    /**
     * Checks the transform configuration is valid
     * <ol>
     * <li>Checks there are the correct number of inputs for a given transform
     * type and that those inputs are not empty strings</li>
     * <li>Check the number of arguments is correct for the transform type and
     * verify the argument (i.e. is is a valid regex)</li>
     * <li>Check there is a valid number of ouputs for the transform type and
     * those outputs are not empty strings</li>
     * <li>If the transform has a condition verify it</li>
     * </ol>
     */
    public static boolean verify(TransformConfig tc) throws ElasticsearchParseException {
        TransformType type;
        try {
            type = tc.type();
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException(Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_UNKNOWN_TYPE, tc.getTransform()));
        }

        checkCondition(tc, type);
        checkInputs(tc, type);
        checkArguments(tc, type);
        checkOutputs(tc, type);

        return true;
    }

    private static void checkCondition(TransformConfig tc, TransformType type) {
        if (type.hasCondition()) {
            if (tc.getCondition() == null) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_CONDITION_REQUIRED, type.prettyName()));
            }
        }
    }

    private static void checkInputs(TransformConfig tc, TransformType type) {
        List<String> inputs = tc.getInputs();
        checkValidInputCount(tc, type, inputs);
        checkInputsAreNonEmptyStrings(tc, inputs);
    }

    private static void checkValidInputCount(TransformConfig tc, TransformType type, List<String> inputs) {
        int inputsSize = (inputs == null) ? 0 : inputs.size();
        if (!type.arityRange().contains(inputsSize)) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INVALID_INPUT_COUNT,
                    tc.getTransform(), rangeAsString(type.arityRange()), inputsSize);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkInputsAreNonEmptyStrings(TransformConfig tc, List<String> inputs) {
        if (containsEmptyString(inputs)) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INPUTS_CONTAIN_EMPTY_STRING, tc.getTransform());
            throw new IllegalArgumentException(msg);
        }
    }

    private static boolean containsEmptyString(List<String> strings) {
        return strings.stream().anyMatch(s -> s.trim().isEmpty());
    }

    private static void checkArguments(TransformConfig tc, TransformType type) {
        checkArgumentsCountValid(tc, type);
        checkArgumentsValid(tc, type);
    }

    private static void checkArgumentsCountValid(TransformConfig tc, TransformType type) {
        List<String> arguments = tc.getArguments();
        int argumentsSize = (arguments == null) ? 0 : arguments.size();
        if (!type.argumentsRange().contains(argumentsSize)) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INVALID_ARGUMENT_COUNT,
                    tc.getTransform(), rangeAsString(type.argumentsRange()), argumentsSize);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkArgumentsValid(TransformConfig tc, TransformType type) {
        if (tc.getArguments() != null) {
            ArgumentVerifier av = argumentVerifierForType(type);
            for (String argument : tc.getArguments()) {
                av.verify(argument, tc);
            }
        }
    }

    private static ArgumentVerifier argumentVerifierForType(TransformType type) {
        switch (type) {
        case REGEX_EXTRACT:
            return new RegexExtractVerifier();
        case REGEX_SPLIT:
            return new RegexPatternVerifier();
        default:
            return (argument, config) -> {};
        }
    }


    private static void checkOutputs(TransformConfig tc, TransformType type) {
        List<String> outputs = tc.getOutputs();
        checkValidOutputCount(tc, type, outputs);
        checkOutputsAreNonEmptyStrings(tc, outputs);
    }

    private static void checkValidOutputCount(TransformConfig tc, TransformType type, List<String> outputs) {
        int outputsSize = (outputs == null) ? 0 : outputs.size();
        if (!type.outputsRange().contains(outputsSize)) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INVALID_OUTPUT_COUNT,
                    tc.getTransform(), rangeAsString(type.outputsRange()), outputsSize);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkOutputsAreNonEmptyStrings(TransformConfig tc, List<String> outputs) {
        if (containsEmptyString(outputs)) {
            String msg = Messages.getMessage(
                    Messages.JOB_CONFIG_TRANSFORM_OUTPUTS_CONTAIN_EMPTY_STRING, tc.getTransform());
            throw new IllegalArgumentException(msg);
        }
    }

    private static String rangeAsString(IntRange range) {
        if (range.hasLowerBound() && range.hasUpperBound() && range.lower() == range.upper()) {
            return String.valueOf(range.lower());
        }
        return range.toString();
    }
}
