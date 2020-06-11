/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.LogisticRegression;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.EnsembleInferenceModel;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.sizeOfCollection;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfDoubleArray;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfStringCollection;

public class EnsembleSizeInfo implements TrainedModelSizeInfo {

    public static final ParseField NAME = new ParseField("ensemble_model_size");
    private static final ParseField TREE_SIZES = new ParseField("tree_sizes");
    private static final ParseField INPUT_FIELD_NAME_LENGHTS = new ParseField("input_field_name_lengths");
    private static final ParseField NUM_OUTPUT_PROCESSOR_WEIGHTS = new ParseField("num_output_processor_weights");
    private static final ParseField NUM_CLASSIFICATION_WEIGHTS = new ParseField("num_classification_weights");
    private static final ParseField NUM_OPERATIONS = new ParseField("num_operations");
    private static final ParseField NUM_CLASSES = new ParseField("num_classes");

    @SuppressWarnings("unchecked")
    static ConstructingObjectParser<EnsembleSizeInfo, Void> PARSER = new ConstructingObjectParser<>(
        "ensemble_size",
        false,
        a -> new EnsembleSizeInfo((List<TreeSizeInfo>)a[0],
            (Integer)a[1],
            (List<Integer>)a[2],
            a[3] == null ? 0 : (Integer)a[3],
            a[4] == null ? 0 : (Integer)a[4],
            a[5] == null ? 0 : (Integer)a[5])
    );
    static {
        PARSER.declareObjectArray(constructorArg(), TreeSizeInfo.PARSER::apply, TREE_SIZES);
        PARSER.declareInt(constructorArg(), NUM_OPERATIONS);
        PARSER.declareIntArray(constructorArg(), INPUT_FIELD_NAME_LENGHTS);
        PARSER.declareInt(optionalConstructorArg(), NUM_OUTPUT_PROCESSOR_WEIGHTS);
        PARSER.declareInt(optionalConstructorArg(), NUM_CLASSIFICATION_WEIGHTS);
        PARSER.declareInt(optionalConstructorArg(), NUM_CLASSES);
    }

    public static EnsembleSizeInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }


    private final List<TreeSizeInfo> treeSizeInfos;
    private final int numOperations;
    private final int[] inputFieldNameLengths;
    private final int numOutputProcessorWeights;
    private final int numClassificationWeights;
    private final int numClasses;

    public EnsembleSizeInfo(List<TreeSizeInfo> treeSizeInfos,
                            int numOperations,
                            List<Integer> inputFieldNameLengths,
                            int numOutputProcessorWeights,
                            int numClassificationWeights,
                            int numClasses) {
        this.treeSizeInfos = treeSizeInfos;
        this.numOperations = numOperations;
        this.inputFieldNameLengths = inputFieldNameLengths.stream().mapToInt(Integer::intValue).toArray();
        this.numOutputProcessorWeights = numOutputProcessorWeights;
        this.numClassificationWeights = numClassificationWeights;
        this.numClasses = numClasses;
    }

    public int getNumOperations() {
        return numOperations;
    }

    @Override
    public long ramBytesUsed() {
        long size = EnsembleInferenceModel.SHALLOW_SIZE;
        treeSizeInfos.forEach(t -> t.setNumClasses(numClasses).ramBytesUsed());
        size += sizeOfCollection(treeSizeInfos);
        size += sizeOfStringCollection(inputFieldNameLengths);
        size += LogisticRegression.SHALLOW_SIZE + sizeOfDoubleArray(numOutputProcessorWeights);
        size += sizeOfDoubleArray(numClassificationWeights);
        return alignObjectSize(size);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TREE_SIZES.getPreferredName(), treeSizeInfos);
        builder.field(NUM_OPERATIONS.getPreferredName(), numOperations);
        builder.field(NUM_CLASSES.getPreferredName(), numClasses);
        builder.field(INPUT_FIELD_NAME_LENGHTS.getPreferredName(), inputFieldNameLengths);
        builder.field(NUM_CLASSIFICATION_WEIGHTS.getPreferredName(), numClassificationWeights);
        builder.field(NUM_OUTPUT_PROCESSOR_WEIGHTS.getPreferredName(), numOutputProcessorWeights);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnsembleSizeInfo that = (EnsembleSizeInfo) o;
        return numOperations == that.numOperations &&
            numOutputProcessorWeights == that.numOutputProcessorWeights &&
            numClassificationWeights == that.numClassificationWeights &&
            numClasses == that.numClasses &&
            Objects.equals(treeSizeInfos, that.treeSizeInfos) &&
            Arrays.equals(inputFieldNameLengths, that.inputFieldNameLengths);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(treeSizeInfos, numOperations, numOutputProcessorWeights, numClassificationWeights, numClasses);
        result = 31 * result + Arrays.hashCode(inputFieldNameLengths);
        return result;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }
}
