/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ModelSizeInfo implements Accountable, ToXContentObject {

    private static final ParseField PREPROCESSORS = new ParseField("preprocessors");
    private static final ParseField TRAINED_MODEL_SIZE = new ParseField("trained_model_size");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ModelSizeInfo, Void> PARSER = new ConstructingObjectParser<>(
        "model_size",
        false,
        a -> new ModelSizeInfo((EnsembleSizeInfo)a[0], (List<PreprocessorSize>)a[1])
    );
    static {
        PARSER.declareNamedObject(constructorArg(),
            (p, c, n) -> p.namedObject(TrainedModelSizeInfo.class, n, null),
            TRAINED_MODEL_SIZE);
        PARSER.declareNamedObjects(optionalConstructorArg(),
            (p, c, n) -> p.namedObject(PreprocessorSize.class, n, null),
            (val) -> {},
            PREPROCESSORS);
    }

    private final EnsembleSizeInfo ensembleSizeInfo;
    private final List<PreprocessorSize> preprocessorSizes;

    public ModelSizeInfo(EnsembleSizeInfo ensembleSizeInfo, List<PreprocessorSize> preprocessorSizes) {
        this.ensembleSizeInfo = ensembleSizeInfo;
        this.preprocessorSizes = preprocessorSizes == null ? Collections.emptyList() : preprocessorSizes;
    }

    public int numOperations() {
        return this.preprocessorSizes.size() + this.ensembleSizeInfo.getNumOperations();
    }

    @Override
    public long ramBytesUsed() {
        long size = InferenceDefinition.SHALLOW_SIZE;
        size += ensembleSizeInfo.ramBytesUsed();
        size += preprocessorSizes.stream().mapToLong(PreprocessorSize::ramBytesUsed).sum();
        return alignObjectSize(size);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        NamedXContentObjectHelper.writeNamedObject(builder, params, TRAINED_MODEL_SIZE.getPreferredName(), ensembleSizeInfo);
        if (preprocessorSizes.size() > 0) {
            NamedXContentObjectHelper.writeNamedObjects(builder, params, true, PREPROCESSORS.getPreferredName(), preprocessorSizes);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelSizeInfo modelSizeInfo = (ModelSizeInfo) o;
        return Objects.equals(ensembleSizeInfo, modelSizeInfo.ensembleSizeInfo) &&
            Objects.equals(preprocessorSizes, modelSizeInfo.preprocessorSizes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ensembleSizeInfo, preprocessorSizes);
    }
}
