/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TrainedModelDefinitionTests extends AbstractSerializingTestCase<TrainedModelDefinition> {

    @Override
    protected TrainedModelDefinition doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinition.fromXContent(parser, true).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public static TrainedModelDefinition.Builder createRandomBuilder(TargetType targetType) {
        int numberOfProcessors = randomIntBetween(1, 10);
        return new TrainedModelDefinition.Builder().setPreProcessors(
            randomBoolean()
                ? null
                : Stream.generate(
                    () -> randomFrom(
                        FrequencyEncodingTests.createRandom(),
                        OneHotEncodingTests.createRandom(),
                        TargetMeanEncodingTests.createRandom()
                    )
                ).limit(numberOfProcessors).collect(Collectors.toList())
        ).setTrainedModel(randomFrom(TreeTests.createRandom(targetType), EnsembleTests.createRandom(targetType)));
    }

    public static TrainedModelDefinition.Builder createRandomBuilder() {
        return createRandomBuilder(randomFrom(TargetType.values()));
    }

    public static final String ENSEMBLE_MODEL = """
        {
          "preprocessors": [
            {
              "one_hot_encoding": {
                "field": "col1",
                "hot_map": {
                  "male": "col1_male",
                  "female": "col1_female"
                }
              }
            },
            {
              "target_mean_encoding": {
                "field": "col2",
                "feature_name": "col2_encoded",
                "target_map": {
                  "S": 5.0,
                  "M": 10.0,
                  "L": 20
                },
                "default_value": 5.0
              }
            },
            {
              "frequency_encoding": {
                "field": "col3",
                "feature_name": "col3_encoded",
                "frequency_map": {
                  "none": 0.75,
                  "true": 0.10,
                  "false": 0.15
                }
              }
            }
          ],
          "trained_model": {
            "ensemble": {
              "feature_names": [
                "col1_male",
                "col1_female",
                "col2_encoded",
                "col3_encoded",
                "col4"
              ],
              "aggregate_output": {
                "weighted_sum": {
                  "weights": [
                    0.5,
                    0.5
                  ]
                }
              },
              "target_type": "regression",
              "trained_models": [
                {
                  "tree": {
                    "feature_names": [
                      "col1_male",
                      "col1_female",
                      "col4"
                    ],
                    "tree_structure": [
                      {
                        "node_index": 0,
                        "split_feature": 0,
                        "split_gain": 12.0,
                        "threshold": 10.0,
                        "decision_type": "lte",
                        "default_left": true,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "leaf_value": 2
                      }
                    ],
                    "target_type": "regression"
                  }
                },
                {
                  "tree": {
                    "feature_names": [
                      "col2_encoded",
                      "col3_encoded",
                      "col4"
                    ],
                    "tree_structure": [
                      {
                        "node_index": 0,
                        "split_feature": 0,
                        "split_gain": 12.0,
                        "threshold": 10.0,
                        "decision_type": "lte",
                        "default_left": true,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "leaf_value": 2
                      }
                    ],
                    "target_type": "regression"
                  }
                }
              ]
            }
          }
        }""";

    public static final String TREE_MODEL = """
        {
          "preprocessors": [
            {
              "one_hot_encoding": {
                "field": "col1",
                "hot_map": {
                  "male": "col1_male",
                  "female": "col1_female"
                }
              }
            },
            {
              "target_mean_encoding": {
                "field": "col2",
                "feature_name": "col2_encoded",
                "target_map": {
                  "S": 5.0,
                  "M": 10.0,
                  "L": 20
                },
                "default_value": 5.0
              }
            },
            {
              "frequency_encoding": {
                "field": "col3",
                "feature_name": "col3_encoded",
                "frequency_map": {
                  "none": 0.75,
                  "true": 0.10,
                  "false": 0.15
                }
              }
            }
          ],
          "trained_model": {
            "tree": {
              "feature_names": [
                "col1_male",
                "col1_female",
                "col4"
              ],
              "tree_structure": [
                {
                  "node_index": 0,
                  "split_feature": 0,
                  "split_gain": 12.0,
                  "threshold": 10.0,
                  "decision_type": "lte",
                  "default_left": true,
                  "left_child": 1,
                  "right_child": 2
                },
                {
                  "node_index": 1,
                  "leaf_value": 1
                },
                {
                  "node_index": 2,
                  "leaf_value": 2
                }
              ],
              "target_type": "regression"
            }
          }
        }""";

    public void testEnsembleSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), ENSEMBLE_MODEL);
        TrainedModelDefinition definition = TrainedModelDefinition.fromXContent(parser, false).build();
        assertThat(definition.getTrainedModel().getClass(), equalTo(Ensemble.class));
    }

    public void testTreeSchemaDeserialization() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), TREE_MODEL);
        TrainedModelDefinition definition = TrainedModelDefinition.fromXContent(parser, false).build();
        assertThat(definition.getTrainedModel().getClass(), equalTo(Tree.class));
    }

    @Override
    protected TrainedModelDefinition createTestInstance() {
        return createRandomBuilder().build();
    }

    @Override
    protected Writeable.Reader<TrainedModelDefinition> instanceReader() {
        return TrainedModelDefinition::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testRamUsageEstimation() {
        TrainedModelDefinition test = createTestInstance();
        assertThat(test.ramBytesUsed(), greaterThan(0L));
    }
}
