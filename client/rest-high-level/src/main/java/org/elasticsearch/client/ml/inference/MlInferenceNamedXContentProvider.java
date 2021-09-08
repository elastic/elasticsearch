/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.preprocessing.CustomWordEmbedding;
import org.elasticsearch.client.ml.inference.preprocessing.Multi;
import org.elasticsearch.client.ml.inference.preprocessing.NGram;
import org.elasticsearch.client.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.client.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Exponent;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.LogisticRegression;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.OutputAggregator;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.client.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.client.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.client.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.client.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.client.ml.inference.preprocessing.TargetMeanEncoding;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.ArrayList;
import java.util.List;

public class MlInferenceNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        // PreProcessing
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(OneHotEncoding.NAME),
            OneHotEncoding::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(TargetMeanEncoding.NAME),
            TargetMeanEncoding::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(FrequencyEncoding.NAME),
            FrequencyEncoding::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(CustomWordEmbedding.NAME),
            CustomWordEmbedding::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(NGram.NAME),
            NGram::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(PreProcessor.class, new ParseField(Multi.NAME),
            Multi::fromXContent));

        // Model
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class, new ParseField(Tree.NAME), Tree::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class, new ParseField(Ensemble.NAME), Ensemble::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class,
            new ParseField(LangIdentNeuralNetwork.NAME),
            LangIdentNeuralNetwork::fromXContent));

        // Inference Config
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceConfig.class,
            ClassificationConfig.NAME,
            ClassificationConfig::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceConfig.class,
            RegressionConfig.NAME,
            RegressionConfig::fromXContent));

        // Aggregating output
        namedXContent.add(new NamedXContentRegistry.Entry(OutputAggregator.class,
            new ParseField(WeightedMode.NAME),
            WeightedMode::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(OutputAggregator.class,
            new ParseField(WeightedSum.NAME),
            WeightedSum::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(OutputAggregator.class,
            new ParseField(LogisticRegression.NAME),
            LogisticRegression::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(OutputAggregator.class,
            new ParseField(Exponent.NAME),
            Exponent::fromXContent));

        // location
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModelLocation.class,
            new ParseField(IndexLocation.INDEX),
            IndexLocation::fromXContent));

        return namedXContent;
    }

}
