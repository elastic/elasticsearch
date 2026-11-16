/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractChunkedBWCSerializationTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class DecisionResultsTests extends AbstractChunkedBWCSerializationTestCase<DecisionResults> {

    public void testToXContent_CreatesTheRightFormat() {
        var answers = new LinkedHashMap<String, DecisionResults.Answer>();
        answers.put("is_complaint", new DecisionResults.PredicateAnswer(0.94));
        answers.put(
            "team",
            new DecisionResults.ChoiceAnswer("billing", orderedMap("billing", 0.91, "shipping", 0.04, "other", 0.05), 0.89)
        );
        answers.put("anger", new DecisionResults.ScoreAnswer(1.4, orderedMap("calm", 0.1, "annoyed", 0.4, "furious", 0.5), 0.62));
        var results = new DecisionResults(answers);

        assertThat(
            results.asMap(),
            is(
                Map.of(
                    DecisionResults.DECISION,
                    Map.of(
                        DecisionResults.ANSWERS,
                        Map.of(
                            "is_complaint",
                            Map.of("type", "predicate", "probability", 0.94),
                            "team",
                            Map.of(
                                "type",
                                "choice",
                                "choice",
                                "billing",
                                "probabilities",
                                List.of(
                                    Map.of("value", "billing", "probability", 0.91),
                                    Map.of("value", "shipping", "probability", 0.04),
                                    Map.of("value", "other", "probability", 0.05)
                                ),
                                "confidence",
                                0.89
                            ),
                            "anger",
                            Map.of(
                                "type",
                                "score",
                                "score",
                                1.4,
                                "probabilities",
                                List.of(
                                    Map.of("value", 0, "label", "calm", "probability", 0.1),
                                    Map.of("value", 1, "label", "annoyed", "probability", 0.4),
                                    Map.of("value", 2, "label", "furious", "probability", 0.5)
                                ),
                                "confidence",
                                0.62
                            )
                        )
                    )
                )
            )
        );

        String xContentResult = Strings.toTruncatedString(results, true, true);
        assertThat(xContentResult, is("""
            {
              "decision" : {
                "answers" : {
                  "is_complaint" : {
                    "type" : "predicate",
                    "probability" : 0.94
                  },
                  "team" : {
                    "type" : "choice",
                    "choice" : "billing",
                    "probabilities" : [
                      {
                        "value" : "billing",
                        "probability" : 0.91
                      },
                      {
                        "value" : "shipping",
                        "probability" : 0.04
                      },
                      {
                        "value" : "other",
                        "probability" : 0.05
                      }
                    ],
                    "confidence" : 0.89
                  },
                  "anger" : {
                    "type" : "score",
                    "score" : 1.4,
                    "probabilities" : [
                      {
                        "value" : 0,
                        "label" : "calm",
                        "probability" : 0.1
                      },
                      {
                        "value" : 1,
                        "label" : "annoyed",
                        "probability" : 0.4
                      },
                      {
                        "value" : 2,
                        "label" : "furious",
                        "probability" : 0.5
                      }
                    ],
                    "confidence" : 0.62
                  }
                }
              }
            }"""));
    }

    public void testParser_WithMissingAnswerField_Throws() throws IOException {
        var json = """
            {"decision": {"answers": {"team": {"type": "choice", "choice": "billing", "confidence": 0.89}}}}
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var exception = expectThrows(IllegalArgumentException.class, () -> DecisionResults.createParser(false).apply(parser, null));
            var messages = new StringBuilder();
            for (Throwable t = exception; t != null; t = t.getCause()) {
                messages.append(t.getMessage()).append('\n');
            }
            assertThat(messages.toString(), containsString("Field [probabilities] is required for answers of type [choice]"));
        }
    }

    private static Map<String, Double> orderedMap(String k1, double v1, String k2, double v2, String k3, double v3) {
        var map = new LinkedHashMap<String, Double>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return map;
    }

    @Override
    protected Writeable.Reader<DecisionResults> instanceReader() {
        return DecisionResults::new;
    }

    @Override
    protected DecisionResults createTestInstance() {
        return createRandomResults();
    }

    public static DecisionResults createRandomResults() {
        var answers = new LinkedHashMap<String, DecisionResults.Answer>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            answers.put(randomAlphaOfLength(8) + i, createRandomAnswer());
        }
        return new DecisionResults(answers);
    }

    private static DecisionResults.Answer createRandomAnswer() {
        return switch (randomFrom(DecisionQuestion.Type.values())) {
            case PREDICATE -> new DecisionResults.PredicateAnswer(randomDouble());
            case CHOICE -> {
                var probabilities = randomProbabilities();
                yield new DecisionResults.ChoiceAnswer(randomFrom(List.copyOf(probabilities.keySet())), probabilities, randomDouble());
            }
            case SCORE -> new DecisionResults.ScoreAnswer(randomDoubleBetween(0, 4, true), randomProbabilities(), randomDouble());
        };
    }

    private static Map<String, Double> randomProbabilities() {
        var probabilities = new LinkedHashMap<String, Double>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            probabilities.put(randomAlphaOfLength(8) + i, randomDouble());
        }
        return probabilities;
    }

    @Override
    protected DecisionResults mutateInstance(DecisionResults instance) throws IOException {
        var answers = new LinkedHashMap<>(instance.answers());
        if (answers.isEmpty() == false && randomBoolean()) {
            answers.remove(randomFrom(answers.keySet()));
        } else {
            answers.put(randomAlphaOfLength(10), createRandomAnswer());
        }
        return new DecisionResults(answers);
    }

    @Override
    protected DecisionResults mutateInstanceForVersion(DecisionResults instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected DecisionResults doParseInstance(XContentParser parser) throws IOException {
        return DecisionResults.createParser(false).apply(parser, null);
    }
}
