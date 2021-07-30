/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.CustomWordEmbedding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.Multi;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.NGram;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SentimentAnalysisResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Exponent;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.LenientlyParsedOutputAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.LogisticRegression;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.OutputAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.StrictlyParsedOutputAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.EnsembleInferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;

import java.util.ArrayList;
import java.util.List;

public class MlInferenceNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        // PreProcessing Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, OneHotEncoding.NAME,
            (p, c) -> OneHotEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, TargetMeanEncoding.NAME,
            (p, c) -> TargetMeanEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, FrequencyEncoding.NAME,
            (p, c) -> FrequencyEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, CustomWordEmbedding.NAME,
            (p, c) -> CustomWordEmbedding.fromXContentLenient(p)));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, NGram.NAME,
            (p, c) -> NGram.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, Multi.NAME,
            (p, c) -> Multi.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)));

        // PreProcessing Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, OneHotEncoding.NAME,
            (p, c) -> OneHotEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, TargetMeanEncoding.NAME,
            (p, c) -> TargetMeanEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, FrequencyEncoding.NAME,
            (p, c) -> FrequencyEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, CustomWordEmbedding.NAME,
            (p, c) -> CustomWordEmbedding.fromXContentStrict(p)));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, NGram.NAME,
            (p, c) -> NGram.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, Multi.NAME,
            (p, c) -> Multi.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)));

        // Model Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModel.class, Tree.NAME, Tree::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModel.class, Ensemble.NAME, Ensemble::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModel.class,
            LangIdentNeuralNetwork.NAME,
            LangIdentNeuralNetwork::fromXContentLenient));

        // Output Aggregator Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class,
            WeightedMode.NAME,
            WeightedMode::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class,
            WeightedSum.NAME,
            WeightedSum::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class,
            LogisticRegression.NAME,
            LogisticRegression::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class,
            Exponent.NAME,
            Exponent::fromXContentLenient));

        // Model Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModel.class, Tree.NAME, Tree::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModel.class, Ensemble.NAME, Ensemble::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModel.class,
            LangIdentNeuralNetwork.NAME,
            LangIdentNeuralNetwork::fromXContentStrict));

        // Output Aggregator Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class,
            WeightedMode.NAME,
            WeightedMode::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class,
            WeightedSum.NAME,
            WeightedSum::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class,
            LogisticRegression.NAME,
            LogisticRegression::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class,
            Exponent.NAME,
            Exponent::fromXContentStrict));

        // Location lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModelLocation.class,
            IndexLocation.INDEX,
            IndexLocation::fromXContentLenient));

        // Location strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModelLocation.class,
            IndexLocation.INDEX,
            IndexLocation::fromXContentStrict));

        // Inference Configs
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedInferenceConfig.class, ClassificationConfig.NAME,
                ClassificationConfig::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedInferenceConfig.class, ClassificationConfig.NAME,
            ClassificationConfig::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedInferenceConfig.class, RegressionConfig.NAME,
                RegressionConfig::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedInferenceConfig.class, RegressionConfig.NAME,
            RegressionConfig::fromXContentStrict));

        namedXContent.add(new NamedXContentRegistry.Entry(InferenceConfigUpdate.class, ClassificationConfigUpdate.NAME,
            ClassificationConfigUpdate::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceConfigUpdate.class, RegressionConfigUpdate.NAME,
            RegressionConfigUpdate::fromXContentStrict));

        // Inference models
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceModel.class, Ensemble.NAME, EnsembleInferenceModel::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceModel.class, Tree.NAME, TreeInferenceModel::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceModel.class,
            LangIdentNeuralNetwork.NAME,
            LangIdentNeuralNetwork::fromXContentLenient));

        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // PreProcessing
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, OneHotEncoding.NAME.getPreferredName(),
            OneHotEncoding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, TargetMeanEncoding.NAME.getPreferredName(),
            TargetMeanEncoding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, FrequencyEncoding.NAME.getPreferredName(),
            FrequencyEncoding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, CustomWordEmbedding.NAME.getPreferredName(),
            CustomWordEmbedding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, NGram.NAME.getPreferredName(),
            NGram::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, Multi.NAME.getPreferredName(),
            Multi::new));

        // Model
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModel.class, Tree.NAME.getPreferredName(), Tree::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModel.class, Ensemble.NAME.getPreferredName(), Ensemble::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModel.class,
            LangIdentNeuralNetwork.NAME.getPreferredName(),
            LangIdentNeuralNetwork::new));

        // Output Aggregator
        namedWriteables.add(new NamedWriteableRegistry.Entry(OutputAggregator.class,
            WeightedSum.NAME.getPreferredName(),
            WeightedSum::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(OutputAggregator.class,
            WeightedMode.NAME.getPreferredName(),
            WeightedMode::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(OutputAggregator.class,
            LogisticRegression.NAME.getPreferredName(),
            LogisticRegression::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(OutputAggregator.class,
            Exponent.NAME.getPreferredName(),
            Exponent::new));

        // Inference Results
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            ClassificationInferenceResults.NAME,
            ClassificationInferenceResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            RegressionInferenceResults.NAME,
            RegressionInferenceResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            WarningInferenceResults.NAME,
            WarningInferenceResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            NerResults.NAME,
            NerResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            FillMaskResults.NAME,
            FillMaskResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            PyTorchPassThroughResults.NAME,
            PyTorchPassThroughResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class,
            SentimentAnalysisResults.NAME,
            SentimentAnalysisResults::new));

        // Inference Configs
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class,
            ClassificationConfig.NAME.getPreferredName(), ClassificationConfig::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class,
            RegressionConfig.NAME.getPreferredName(), RegressionConfig::new));

        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class,
            ClassificationConfigUpdate.NAME.getPreferredName(), ClassificationConfigUpdate::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class,
            RegressionConfigUpdate.NAME.getPreferredName(), RegressionConfigUpdate::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class,
            ResultsFieldUpdate.NAME, ResultsFieldUpdate::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class,
            EmptyConfigUpdate.NAME, EmptyConfigUpdate::new));

        // Location
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModelLocation.class,
            IndexLocation.INDEX.getPreferredName(), IndexLocation::new));

        return namedWriteables;
    }
}
