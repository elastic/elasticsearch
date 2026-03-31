/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class ExampleModels {

    static final String BASE_64_ENCODED_PYTORCH_MODEL =
        "UEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAUAA4Ac2ltcGxlbW9kZWwvZGF0YS5wa2xGQgoAWlpaWlpaWlpaWoACY19fdG9yY2hfXwp"
            + "TdXBlclNpbXBsZQpxACmBfShYCAAAAHRyYWluaW5ncQGIdWJxAi5QSwcIXOpBBDQAAAA0AAAAUEsDBBQACAgIAAAAAAAAAAAAAAAAAA"
            + "AAAAAdAEEAc2ltcGxlbW9kZWwvY29kZS9fX3RvcmNoX18ucHlGQj0AWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaW"
            + "lpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWnWOMWvDMBCF9/yKI5MMrnHTQsHgjt2aJdlCEIp9SgWSTpykFvfXV1htaYds0nfv473Jqhjh"
            + "kAPywbhgUbzSnC02wwZAyqBYOUzIUUoY4XRe6SVr/Q8lVsYbf4UBLkS2kBk1aOIPxbOIaPVQtEQ8vUnZ/WlrSxTA+JCTNHMc4Ig+Ele"
            + "s+Jod+iR3N/jDDf74wxu4e/5+DmtE9mUyhdgFNq7bZ3ekehbruC6aTxS/c1rom6Z698WrEfIYxcn4JGTftLA7tzCnJeD41IJVC+U07k"
            + "umUHw3E47Vqh+xnULeFisYLx064mV8UTZibWFMmX0p23wBUEsHCE0EGH3yAAAAlwEAAFBLAwQUAAgICAAAAAAAAAAAAAAAAAAAAAAAJ"
            + "wA5AHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5LmRlYnVnX3BrbEZCNQBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpa"
            + "WlpaWlpaWlpaWlpaWlpaWlpaWlpaWrWST0+DMBiHW6bOod/BGS94kKpo2Mwyox5x3pbgiXSAFtdR/nQu3IwHiZ9oX88CaeGu9tL0efq"
            + "+v8P7fmiGA1wgTgoIcECZQqe6vmYD6G4hAJOcB1E8NazTm+ELyzY4C3Q0z8MsRwF+j4JlQUPEEo5wjH0WB9hCNFqgpOCExZY5QnnEw7"
            + "ME+0v8GuaIs8wnKI7RigVrKkBzm0lh2OdjkeHllG28f066vK6SfEypF60S+vuYt4gjj2fYr/uPrSvRv356TepfJ9iWJRN0OaELQSZN3"
            + "FRPNbcP1PTSntMr0x0HzLZQjPYIEo3UaFeiISRKH0Mil+BE/dyT1m7tCBLwVO1MX4DK3bbuTlXuy8r71j5Aoho66udAoseOnrdVzx28"
            + "UFW6ROuO/lT6QKKyo79VU54emj9QSwcInsUTEDMBAAAFAwAAUEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAZAAYAc2ltcGxlbW9kZWw"
            + "vY29uc3RhbnRzLnBrbEZCAgBaWoACKS5QSwcIbS8JVwQAAAAEAAAAUEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAATADsAc2ltcGxlbW"
            + "9kZWwvdmVyc2lvbkZCNwBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaMwpQSwcI0"
            + "Z5nVQIAAAACAAAAUEsBAgAAAAAICAAAAAAAAFzqQQQ0AAAANAAAABQAAAAAAAAAAAAAAAAAAAAAAHNpbXBsZW1vZGVsL2RhdGEucGts"
            + "UEsBAgAAFAAICAgAAAAAAE0EGH3yAAAAlwEAAB0AAAAAAAAAAAAAAAAAhAAAAHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5UEs"
            + "BAgAAFAAICAgAAAAAAJ7FExAzAQAABQMAACcAAAAAAAAAAAAAAAAAAgIAAHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5LmRlYn"
            + "VnX3BrbFBLAQIAAAAACAgAAAAAAABtLwlXBAAAAAQAAAAZAAAAAAAAAAAAAAAAAMMDAABzaW1wbGVtb2RlbC9jb25zdGFudHMucGtsU"
            + "EsBAgAAAAAICAAAAAAAANGeZ1UCAAAAAgAAABMAAAAAAAAAAAAAAAAAFAQAAHNpbXBsZW1vZGVsL3ZlcnNpb25QSwYGLAAAAAAAAAAe"
            + "Ay0AAAAAAAAAAAAFAAAAAAAAAAUAAAAAAAAAagEAAAAAAACSBAAAAAAAAFBLBgcAAAAA/AUAAAAAAAABAAAAUEsFBgAAAAAFAAUAagE"
            + "AAJIEAAAAAA==";
    static final long RAW_PYTORCH_MODEL_SIZE; // size of the model before base64 encoding
    static {
        RAW_PYTORCH_MODEL_SIZE = Base64.getDecoder().decode(BASE_64_ENCODED_PYTORCH_MODEL).length;
    }

    static String pytorchPassThroughModelConfig() {
        return """
             {
               "description": "simple model for testing",
               "model_type": "pytorch",
               "inference_config": {
                 "pass_through": {
                   "tokenization": {
                     "bert": {
                       "with_special_tokens": false
                     }
                   }
                 }
               }
             }
            """;
    }

    static String mockServiceModelConfig() {
        return org.elasticsearch.common.Strings.format("""
            {
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """);
    }

    private static final String REGRESSION_DEFINITION = """
        {  "preprocessors": [
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
                        "number_samples": 300,
                        "default_left": true,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "number_samples": 100,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "number_samples": 200,
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
                        "number_samples": 150,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "number_samples": 50,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "number_samples": 100,
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

    public static String boostedTreeRegressionModel() {
        return Strings.format("""
            {
                "input": {
                    "field_names": [
                        "col1",
                        "col2",
                        "col3",
                        "col4"
                    ]
                },
                "description": "test model for regression",
                "inference_config": {
                    "regression": {}
                },
                "definition": %s
            }""", REGRESSION_DEFINITION);
    }

    public static String nlpModelPipelineDefinition(String modelId) {
        return Strings.format("""
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "input_output": {
                      "input_field": "body",
                      "output_field": "ml.body"
                    }
                  }
                }
              ]
            }""", modelId);
    }

    public static String nlpModelPipelineDefinitionWithFieldMap(String modelId) {
        return Strings.format("""
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "field_map": {
                      "body": "input"
                    }
                  }
                }
              ]
            }""", modelId);
    }

    public static String boostedTreeRegressionModelPipelineDefinition(String modelId) {
        return Strings.format("""
            {
                "processors": [
                    {
                        "inference": {
                            "target_field": "ml.regression",
                            "model_id": "%s",
                            "inference_config": {
                                "regression": {}
                            },
                            "field_map": {
                                "col1": "col1",
                                "col2": "col2",
                                "col3": "col3",
                                "col4": "col4"
                            }
                        }
                    }
                ]
            }""", modelId);
    }

    public static String randomBoostedTreeModelDoc() throws IOException {
        Map<String, Object> values = Map.of(
            "col1",
            randomFrom("female", "male"),
            "col2",
            randomFrom("S", "M", "L", "XL"),
            "col3",
            randomFrom("true", "false", "none", "other"),
            "col4",
            randomIntBetween(0, 10)
        );

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(values)) {
            return XContentHelper.convertToJson(BytesReference.bytes(xContentBuilder), false, XContentType.JSON);
        }
    }

    private ExampleModels() {}
}
