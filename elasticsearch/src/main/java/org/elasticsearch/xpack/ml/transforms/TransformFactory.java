/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformIndex;

/**
 * Create transforms from the configuration object.
 * Transforms need to know where to read strings from and where
 * write the output to hence input and output maps required by the
 * create method.
 */
public class TransformFactory {
    public static final int INPUT_ARRAY_INDEX = 0;
    public static final int SCRATCH_ARRAY_INDEX = 1;
    public static final int OUTPUT_ARRAY_INDEX = 2;

    public Transform create(TransformConfig transformConfig,
            Map<String, Integer> inputIndexesMap,
            Map<String, Integer> scratchAreaIndexesMap,
            Map<String, Integer> outputIndexesMap,
            Logger logger) {
        int[] input = new int[transformConfig.getInputs().size()];
        fillIndexArray(transformConfig.getInputs(), inputIndexesMap, input);

        List<TransformIndex> readIndexes = new ArrayList<>();
        for (String field : transformConfig.getInputs()) {
            Integer index = inputIndexesMap.get(field);
            if (index != null) {
                readIndexes.add(new TransformIndex(INPUT_ARRAY_INDEX, index));
            } else {
                index = scratchAreaIndexesMap.get(field);
                if (index != null) {
                    readIndexes.add(new TransformIndex(SCRATCH_ARRAY_INDEX, index));
                } else if (outputIndexesMap.containsKey(field)) { // also check the outputs array for this input
                    index = outputIndexesMap.get(field);
                    readIndexes.add(new TransformIndex(SCRATCH_ARRAY_INDEX, index));
                } else {
                    throw new IllegalStateException("Transform input '" + field +
                            "' cannot be found");
                }
            }
        }

        List<TransformIndex> writeIndexes = new ArrayList<>();
        for (String field : transformConfig.getOutputs()) {
            Integer index = outputIndexesMap.get(field);
            if (index != null) {
                writeIndexes.add(new TransformIndex(OUTPUT_ARRAY_INDEX, index));
            } else {
                index = scratchAreaIndexesMap.get(field);
                if (index != null) {
                    writeIndexes.add(new TransformIndex(SCRATCH_ARRAY_INDEX, index));
                }
            }
        }

        TransformType type = transformConfig.type();

        switch (type) {
        case DOMAIN_SPLIT:
            return new HighestRegisteredDomain(readIndexes, writeIndexes, logger);
        case CONCAT:
            if (transformConfig.getArguments().isEmpty()) {
                return new Concat(readIndexes, writeIndexes, logger);
            } else {
                return new Concat(transformConfig.getArguments().get(0),
                        readIndexes, writeIndexes, logger);
            }
        case REGEX_EXTRACT:
            return new RegexExtract(transformConfig.getArguments().get(0), readIndexes,
                    writeIndexes, logger);
        case REGEX_SPLIT:
            return new RegexSplit(transformConfig.getArguments().get(0), readIndexes,
                    writeIndexes, logger);
        case EXCLUDE:
            if (transformConfig.getCondition().getOperator().expectsANumericArgument()) {
                return new ExcludeFilterNumeric(transformConfig.getCondition(),
                        readIndexes, writeIndexes, logger);
            } else {
                return new ExcludeFilterRegex(transformConfig.getCondition(), readIndexes,
                        writeIndexes, logger);
            }
        case LOWERCASE:
            return StringTransform.createLowerCase(readIndexes, writeIndexes, logger);
        case UPPERCASE:
            return StringTransform.createUpperCase(readIndexes, writeIndexes, logger);
        case TRIM:
            return StringTransform.createTrim(readIndexes, writeIndexes, logger);
        default:
            // This code will never be hit - it's to
            // keep the compiler happy.
            throw new IllegalArgumentException("Unknown transform type " + type);
        }
    }

    /**
     * For each <code>field</code> fill the <code>indexArray</code>
     * with the index from the <code>indexes</code> map.
     */
    private static void fillIndexArray(List<String> fields, Map<String, Integer> indexes,
            int[] indexArray) {
        int i = 0;
        for (String field : fields) {
            Integer index = indexes.get(field);
            if (index != null) {
                indexArray[i++] = index;
            }
        }
    }
}
