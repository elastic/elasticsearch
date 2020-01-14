/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.client.ml.inference.preprocessing.CustomWordEmbedding;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Ensemble;
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
import org.elasticsearch.common.ParseField;
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

        // Model
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class, new ParseField(Tree.NAME), Tree::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class, new ParseField(Ensemble.NAME), Ensemble::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(TrainedModel.class,
            new ParseField(LangIdentNeuralNetwork.NAME),
            LangIdentNeuralNetwork::fromXContent));

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

        return namedXContent;
    }

}
