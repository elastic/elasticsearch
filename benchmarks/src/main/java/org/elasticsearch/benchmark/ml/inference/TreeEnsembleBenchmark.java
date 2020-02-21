/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.ml.inference;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.OutputAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@State(Scope.Benchmark)
public class TreeEnsembleBenchmark {

    private Map<String, Object> airBnbDoc;
    private Map<String, Object> smallDoc;
    private NamedXContentRegistry xContentRegistry;
    private TrainedModelDefinition airBnbModel;
    private InferenceConfig inferenceConfig = new RegressionConfig("results_fff");
    private Ensemble ensemble;

    @Param({"4", "8", "16", "32", "64", "128", "256"})
    private int numTrees;
    @Param({"16"})
    private int treeDepth;

    @Setup
    public void setup() throws IOException {
        airBnbDoc = buildAirBnbDoc();
        smallDoc = Map.of("foo", 10.0, "bar", 4.0);

        xContentRegistry = xContentRegistry();
        airBnbModel = readLargeModel("/Users/dkyle/Development/inference/models/airbnb-ashville/trimmed_trainedmodel.json");

        ensemble = buildRandomEnsemble(List.of("foo", "bar", "baz", "coo"), numTrees, treeDepth);
    }


    @Benchmark
    public InferenceResults inferEnsemble() {
        return ensemble.infer(smallDoc, inferenceConfig);
    }

    @Benchmark
    public InferenceResults inferAirBnbModel() {
        return airBnbModel.infer(airBnbDoc, inferenceConfig);
    }

    private NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }


    public TrainedModelDefinition readLargeModel(String path) throws IOException {

        FileInputStream in = new FileInputStream(path);
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, in);

        TrainedModelConfig config = TrainedModelConfig.fromXContent(parser, true).build();
        return config.ensureParsedDefinition(xContentRegistry).getModelDefinition();
    }

    private Map<String, Object> buildAirBnbDoc() {
        Map<String, Object> mm = new HashMap<>();

        mm.put("listing.bed_type", "Real Bed");
        mm.put("listing.longitude", -0.11732);
        mm.put("listing.room_type", "Private room");
        mm.put("listing.security_deposit", 350.00);
        mm.put("listing.bedrooms", 1);
        mm.put("day_of_month", 12);
        mm.put("listing.accommodates", 4);
        mm.put("listing.extra_people", 20.0);
        mm.put("month", 3);
        mm.put("listing.cleaning_fee", 50);
        mm.put("listing.latitude", 51.46225);
        mm.put("listing.zipcode", 28804);
        mm.put("listing.bathrooms", 1);
        mm.put("listing.beds", 1);
        mm.put("day_of_week", 5);

        return mm;
    }

    private Tree buildRandomTree(List<String> featureNames, int depth) {
        Tree.Builder builder = Tree.builder();
        int maxFeatureIndex = featureNames.size();
        builder.setFeatureNames(featureNames);

        Random rng = new Random();

        TreeNode.Builder node = builder.addJunction(0, rng.nextInt(maxFeatureIndex), true, rng.nextDouble());
        List<Integer> childNodes = List.of(node.getLeftChild(), node.getRightChild());

        for (int i = 0; i < depth -1; i++) {

            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {
                if (i == depth -2) {
                    builder.addLeaf(nodeId, rng.nextDouble());
                } else {
                    TreeNode.Builder childNode =
                            builder.addJunction(nodeId, rng.nextInt(maxFeatureIndex), true, rng.nextDouble());
                    nextNodes.add(childNode.getLeftChild());
                    nextNodes.add(childNode.getRightChild());
                }
            }
            childNodes = nextNodes;
        }

        return builder.setTargetType(TargetType.REGRESSION).build();
    }

    private Ensemble buildRandomEnsemble(List<String> featureNames, int numberOfModels, int depth) {
        Random rng = new Random();

        List<TrainedModel> models = Stream.generate(() -> buildRandomTree(featureNames, depth))
                .limit(numberOfModels)
                .collect(Collectors.toList());

        double[] weights = Stream.generate(() -> rng.nextDouble()).limit(numberOfModels).mapToDouble(Double::valueOf).toArray();

        OutputAggregator outputAggregator = new WeightedSum(weights);

        return new Ensemble(featureNames,
                models,
                outputAggregator,
                TargetType.REGRESSION,
                null,
                null);
    }
}
