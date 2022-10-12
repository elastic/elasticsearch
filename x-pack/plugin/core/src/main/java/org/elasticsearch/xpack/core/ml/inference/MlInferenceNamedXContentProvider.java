/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
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
import org.elasticsearch.xpack.core.ml.inference.results.NlpClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResults;
import org.elasticsearch.xpack.core.ml.inference.results.QuestionAnsweringInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextSimilarityInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenizationUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenizationUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenizationUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdate;
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
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                OneHotEncoding.NAME,
                (p, c) -> OneHotEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                TargetMeanEncoding.NAME,
                (p, c) -> TargetMeanEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                FrequencyEncoding.NAME,
                (p, c) -> FrequencyEncoding.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                CustomWordEmbedding.NAME,
                (p, c) -> CustomWordEmbedding.fromXContentLenient(p)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                NGram.NAME,
                (p, c) -> NGram.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedPreProcessor.class,
                Multi.NAME,
                (p, c) -> Multi.fromXContentLenient(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );

        // PreProcessing Strict
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                OneHotEncoding.NAME,
                (p, c) -> OneHotEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                TargetMeanEncoding.NAME,
                (p, c) -> TargetMeanEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                FrequencyEncoding.NAME,
                (p, c) -> FrequencyEncoding.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                CustomWordEmbedding.NAME,
                (p, c) -> CustomWordEmbedding.fromXContentStrict(p)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                NGram.NAME,
                (p, c) -> NGram.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedPreProcessor.class,
                Multi.NAME,
                (p, c) -> Multi.fromXContentStrict(p, (PreProcessor.PreProcessorParseContext) c)
            )
        );

        // Model Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModel.class, Tree.NAME, Tree::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedTrainedModel.class, Ensemble.NAME, Ensemble::fromXContentLenient));
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedTrainedModel.class,
                LangIdentNeuralNetwork.NAME,
                LangIdentNeuralNetwork::fromXContentLenient
            )
        );

        // Output Aggregator Lenient
        namedXContent.add(
            new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class, WeightedMode.NAME, WeightedMode::fromXContentLenient)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class, WeightedSum.NAME, WeightedSum::fromXContentLenient)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedOutputAggregator.class,
                LogisticRegression.NAME,
                LogisticRegression::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(LenientlyParsedOutputAggregator.class, Exponent.NAME, Exponent::fromXContentLenient)
        );

        // Model Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModel.class, Tree.NAME, Tree::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedTrainedModel.class, Ensemble.NAME, Ensemble::fromXContentStrict));
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedTrainedModel.class,
                LangIdentNeuralNetwork.NAME,
                LangIdentNeuralNetwork::fromXContentStrict
            )
        );

        // Output Aggregator Strict
        namedXContent.add(
            new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class, WeightedMode.NAME, WeightedMode::fromXContentStrict)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class, WeightedSum.NAME, WeightedSum::fromXContentStrict)
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedOutputAggregator.class,
                LogisticRegression.NAME,
                LogisticRegression::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(StrictlyParsedOutputAggregator.class, Exponent.NAME, Exponent::fromXContentStrict)
        );

        // Location lenient
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedTrainedModelLocation.class,
                IndexLocation.INDEX,
                IndexLocation::fromXContentLenient
            )
        );

        // Location strict
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedTrainedModelLocation.class,
                IndexLocation.INDEX,
                IndexLocation::fromXContentStrict
            )
        );

        // Inference Configs
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                ClassificationConfig.NAME,
                ClassificationConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                ClassificationConfig.NAME,
                ClassificationConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                RegressionConfig.NAME,
                RegressionConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                RegressionConfig.NAME,
                RegressionConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(NerConfig.NAME),
                NerConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(NerConfig.NAME),
                NerConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(FillMaskConfig.NAME),
                FillMaskConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(FillMaskConfig.NAME),
                FillMaskConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(TextClassificationConfig.NAME),
                TextClassificationConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(TextClassificationConfig.NAME),
                TextClassificationConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(PassThroughConfig.NAME),
                PassThroughConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(PassThroughConfig.NAME),
                PassThroughConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(TextEmbeddingConfig.NAME),
                TextEmbeddingConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(TextEmbeddingConfig.NAME),
                TextEmbeddingConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(ZeroShotClassificationConfig.NAME),
                ZeroShotClassificationConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(ZeroShotClassificationConfig.NAME),
                ZeroShotClassificationConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(QuestionAnsweringConfig.NAME),
                QuestionAnsweringConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(QuestionAnsweringConfig.NAME),
                QuestionAnsweringConfig::fromXContentLenient
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                new ParseField(TextSimilarityConfig.NAME),
                TextSimilarityConfig::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                new ParseField(TextSimilarityConfig.NAME),
                TextSimilarityConfig::fromXContentLenient
            )
        );

        // Inference Configs Update
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                ClassificationConfigUpdate.NAME,
                ClassificationConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(FillMaskConfigUpdate.NAME),
                FillMaskConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(NerConfigUpdate.NAME),
                NerConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(PassThroughConfigUpdate.NAME),
                PassThroughConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                RegressionConfigUpdate.NAME,
                RegressionConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(TextClassificationConfigUpdate.NAME),
                TextClassificationConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(TextEmbeddingConfigUpdate.NAME),
                TextEmbeddingConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(ZeroShotClassificationConfigUpdate.NAME),
                ZeroShotClassificationConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(QuestionAnsweringConfigUpdate.NAME),
                QuestionAnsweringConfigUpdate::fromXContentStrict
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                new ParseField(TextSimilarityConfigUpdate.NAME),
                TextSimilarityConfigUpdate::fromXContentStrict
            )
        );

        // Inference models
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceModel.class, Ensemble.NAME, EnsembleInferenceModel::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(InferenceModel.class, Tree.NAME, TreeInferenceModel::fromXContent));
        namedXContent.add(
            new NamedXContentRegistry.Entry(InferenceModel.class, LangIdentNeuralNetwork.NAME, LangIdentNeuralNetwork::fromXContentLenient)
        );

        // Tokenization
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Tokenization.class,
                BertTokenization.NAME,
                (p, c) -> BertTokenization.fromXContent(p, (boolean) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Tokenization.class,
                MPNetTokenization.NAME,
                (p, c) -> MPNetTokenization.fromXContent(p, (boolean) c)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                Tokenization.class,
                new ParseField(RobertaTokenization.NAME),
                (p, c) -> RobertaTokenization.fromXContent(p, (boolean) c)
            )
        );

        namedXContent.add(
            new NamedXContentRegistry.Entry(
                TokenizationUpdate.class,
                BertTokenizationUpdate.NAME,
                (p, c) -> BertTokenizationUpdate.fromXContent(p)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                TokenizationUpdate.class,
                MPNetTokenizationUpdate.NAME,
                (p, c) -> MPNetTokenizationUpdate.fromXContent(p)
            )
        );
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                TokenizationUpdate.class,
                RobertaTokenizationUpdate.NAME,
                (p, c) -> RobertaTokenizationUpdate.fromXContent(p)
            )
        );

        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // PreProcessing
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PreProcessor.class, OneHotEncoding.NAME.getPreferredName(), OneHotEncoding::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PreProcessor.class, TargetMeanEncoding.NAME.getPreferredName(), TargetMeanEncoding::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PreProcessor.class, FrequencyEncoding.NAME.getPreferredName(), FrequencyEncoding::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PreProcessor.class, CustomWordEmbedding.NAME.getPreferredName(), CustomWordEmbedding::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, NGram.NAME.getPreferredName(), NGram::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, Multi.NAME.getPreferredName(), Multi::new));

        // Model
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModel.class, Tree.NAME.getPreferredName(), Tree::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(TrainedModel.class, Ensemble.NAME.getPreferredName(), Ensemble::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TrainedModel.class,
                LangIdentNeuralNetwork.NAME.getPreferredName(),
                LangIdentNeuralNetwork::new
            )
        );

        // Output Aggregator
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(OutputAggregator.class, WeightedSum.NAME.getPreferredName(), WeightedSum::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(OutputAggregator.class, WeightedMode.NAME.getPreferredName(), WeightedMode::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(OutputAggregator.class, LogisticRegression.NAME.getPreferredName(), LogisticRegression::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(OutputAggregator.class, Exponent.NAME.getPreferredName(), Exponent::new));

        // Inference Results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceResults.class,
                ClassificationInferenceResults.NAME,
                ClassificationInferenceResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResults.class, RegressionInferenceResults.NAME, RegressionInferenceResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResults.class, WarningInferenceResults.NAME, WarningInferenceResults::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class, NerResults.NAME, NerResults::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class, FillMaskResults.NAME, FillMaskResults::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResults.class, PyTorchPassThroughResults.NAME, PyTorchPassThroughResults::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceResults.class, TextEmbeddingResults.NAME, TextEmbeddingResults::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceResults.class,
                NlpClassificationInferenceResults.NAME,
                NlpClassificationInferenceResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceResults.class,
                QuestionAnsweringInferenceResults.NAME,
                QuestionAnsweringInferenceResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceResults.class,
                TextSimilarityInferenceResults.NAME,
                TextSimilarityInferenceResults::new
            )
        );
        // Inference Configs
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, ClassificationConfig.NAME.getPreferredName(), ClassificationConfig::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, RegressionConfig.NAME.getPreferredName(), RegressionConfig::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class, NerConfig.NAME, NerConfig::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class, FillMaskConfig.NAME, FillMaskConfig::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, TextClassificationConfig.NAME, TextClassificationConfig::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class, PassThroughConfig.NAME, PassThroughConfig::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class, TextEmbeddingConfig.NAME, TextEmbeddingConfig::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, ZeroShotClassificationConfig.NAME, ZeroShotClassificationConfig::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, QuestionAnsweringConfig.NAME, QuestionAnsweringConfig::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfig.class, TextSimilarityConfig.NAME, TextSimilarityConfig::new));

        // Inference Configs Updates
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                ClassificationConfigUpdate.NAME.getPreferredName(),
                ClassificationConfigUpdate::new
            )
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, EmptyConfigUpdate.NAME, EmptyConfigUpdate::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, FillMaskConfigUpdate.NAME, FillMaskConfigUpdate::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, NerConfigUpdate.NAME, NerConfigUpdate::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, PassThroughConfigUpdate.NAME, PassThroughConfigUpdate::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                RegressionConfigUpdate.NAME.getPreferredName(),
                RegressionConfigUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, ResultsFieldUpdate.NAME, ResultsFieldUpdate::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                TextClassificationConfigUpdate.NAME,
                TextClassificationConfigUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, TextEmbeddingConfigUpdate.NAME, TextEmbeddingConfigUpdate::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                ZeroShotClassificationConfigUpdate.NAME,
                ZeroShotClassificationConfigUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                QuestionAnsweringConfigUpdate.NAME,
                QuestionAnsweringConfigUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfigUpdate.class, TextSimilarityConfigUpdate.NAME, TextSimilarityConfigUpdate::new)
        );

        // Location
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TrainedModelLocation.class, IndexLocation.INDEX.getPreferredName(), IndexLocation::new)
        );

        // Tokenization
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(Tokenization.class, BertTokenization.NAME.getPreferredName(), BertTokenization::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(Tokenization.class, MPNetTokenization.NAME.getPreferredName(), MPNetTokenization::new)
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(Tokenization.class, RobertaTokenization.NAME, RobertaTokenization::new));

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TokenizationUpdate.class,
                BertTokenizationUpdate.NAME.getPreferredName(),
                BertTokenizationUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TokenizationUpdate.class,
                MPNetTokenizationUpdate.NAME.getPreferredName(),
                MPNetTokenizationUpdate::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TokenizationUpdate.class,
                RobertaTokenizationUpdate.NAME.getPreferredName(),
                RobertaTokenizationUpdate::new
            )
        );

        return namedWriteables;
    }
}
