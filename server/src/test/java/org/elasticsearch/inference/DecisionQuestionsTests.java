/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class DecisionQuestionsTests extends AbstractBWCSerializationTestCase<DecisionQuestions> {

    public void testParse_WithAllQuestionTypes() throws IOException {
        var json = """
            {
                "is_complaint": {"type": "predicate", "instructions": "Is the customer complaining?"},
                "team": {
                    "type": "choice",
                    "instructions": "Which team should handle this?",
                    "choices": {
                        "billing": "Charges, refunds, invoices",
                        "shipping": "Delivery status, delays, lost packages",
                        "other": null
                    }
                },
                "anger": {
                    "type": "score",
                    "instructions": "How angry is the customer?",
                    "levels": ["calm", {"label": "annoyed", "description": "Mildly upset"}, "furious"]
                }
            }
            """;

        var choices = new LinkedHashMap<String, String>();
        choices.put("billing", "Charges, refunds, invoices");
        choices.put("shipping", "Delivery status, delays, lost packages");
        choices.put("other", null);

        var questions = parse(json, false);
        assertThat(
            questions.questions(),
            is(
                Map.of(
                    "is_complaint",
                    new DecisionQuestion.Predicate("Is the customer complaining?"),
                    "team",
                    new DecisionQuestion.Choice("Which team should handle this?", choices),
                    "anger",
                    new DecisionQuestion.Score(
                        "How angry is the customer?",
                        List.of(
                            new DecisionQuestion.Level("calm"),
                            new DecisionQuestion.Level("annoyed", "Mildly upset"),
                            new DecisionQuestion.Level("furious")
                        )
                    )
                )
            )
        );
        // The order of the questions and choices is preserved
        assertThat(List.copyOf(questions.questions().keySet()), is(List.of("is_complaint", "team", "anger")));
        var team = (DecisionQuestion.Choice) questions.questions().get("team");
        assertThat(List.copyOf(team.choices().keySet()), is(List.of("billing", "shipping", "other")));
    }

    public void testParse_WithNoQuestions_Throws() {
        assertParseFailure("{}", "Field [questions] must contain at least one question");
    }

    public void testParse_WithUnknownType_Throws() {
        assertParseFailure("""
            {"q": {"type": "noul", "instructions": "Is it true?"}}
            """, "Unknown question [type] value [noul]. Supported values are [predicate, choice, score]");
    }

    public void testParse_WithMissingType_Throws() {
        assertParseFailure("""
            {"q": {"instructions": "Is it true?"}}
            """, "Required [type]");
    }

    public void testParse_WithMissingInstructions_Throws() {
        assertParseFailure("""
            {"q": {"type": "predicate"}}
            """, "Required [instructions]");
    }

    public void testParse_WithBlankInstructions_Throws() {
        assertParseFailure("""
            {"q": {"type": "predicate", "instructions": " "}}
            """, "Field [instructions] must be a non-empty string");
    }

    public void testParse_PredicateWithChoices_Throws() {
        assertParseFailure("""
            {"q": {"type": "predicate", "instructions": "Is it true?", "choices": {"a": null, "b": null}}}
            """, "Field [choices] is not supported for questions of type [predicate]");
    }

    public void testParse_PredicateWithLevels_Throws() {
        assertParseFailure("""
            {"q": {"type": "predicate", "instructions": "Is it true?", "levels": ["low", "high"]}}
            """, "Field [levels] is not supported for questions of type [predicate]");
    }

    public void testParse_ChoiceWithoutChoices_Throws() {
        assertParseFailure("""
            {"q": {"type": "choice", "instructions": "Which one?"}}
            """, "Field [choices] is required for questions of type [choice]");
    }

    public void testParse_ChoiceWithLevels_Throws() {
        assertParseFailure("""
            {"q": {"type": "choice", "instructions": "Which one?", "choices": {"a": null, "b": null}, "levels": ["low", "high"]}}
            """, "Field [levels] is not supported for questions of type [choice]");
    }

    public void testParse_ChoiceWithSingleChoice_Throws() {
        assertParseFailure("""
            {"q": {"type": "choice", "instructions": "Which one?", "choices": {"a": null}}}
            """, "Field [choices] must contain at least [2] choices, but found [1]");
    }

    public void testParse_ChoiceWithNonStringDescription_Throws() {
        assertParseFailure("""
            {"q": {"type": "choice", "instructions": "Which one?", "choices": {"a": 1, "b": null}}}
            """, "The description of choice [a] must be a string or null, but found [VALUE_NUMBER]");
    }

    public void testParse_ScoreWithoutLevels_Throws() {
        assertParseFailure("""
            {"q": {"type": "score", "instructions": "How much?"}}
            """, "Field [levels] is required for questions of type [score]");
    }

    public void testParse_ScoreWithChoices_Throws() {
        assertParseFailure("""
            {"q": {"type": "score", "instructions": "How much?", "levels": ["low", "high"], "choices": {"a": null, "b": null}}}
            """, "Field [choices] is not supported for questions of type [score]");
    }

    public void testParse_ScoreWithSingleLevel_Throws() {
        assertParseFailure("""
            {"q": {"type": "score", "instructions": "How much?", "levels": ["low"]}}
            """, "Field [levels] must contain at least [2] levels, but found [1]");
    }

    public void testParse_ScoreWithEmptyLevelLabel_Throws() {
        assertParseFailure("""
            {"q": {"type": "score", "instructions": "How much?", "levels": ["low", {"label": " ", "description": "none"}]}}
            """, "Field [levels] must not contain empty levels");
    }

    public void testParse_ScoreWithDuplicateLevels_Throws() {
        assertParseFailure("""
            {"q": {"type": "score", "instructions": "How much?", "levels": ["low", "high", "low"]}}
            """, "Field [levels] must not contain duplicate levels");
    }

    public void testParse_WithUnknownField_ThrowsForRequests() {
        assertParseFailure("""
            {"q": {"type": "predicate", "instructions": "Is it true?", "unknown": "field"}}
            """, "unknown field [unknown]");
    }

    public void testParse_WithUnknownField_IgnoresFieldForPersistedQuestions() throws IOException {
        var questions = parse("""
            {"q": {"type": "predicate", "instructions": "Is it true?", "unknown": "field"}}
            """, true);
        assertThat(questions.questions(), is(Map.of("q", new DecisionQuestion.Predicate("Is it true?"))));
    }

    public void testConstructor_WithBlankQuestionName_Throws() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DecisionQuestions(Map.of(" ", new DecisionQuestion.Predicate("Is it true?")))
        );
        assertThat(exception.getMessage(), is("Field [questions] must not contain questions with an empty name"));
    }

    private DecisionQuestions parse(String json, boolean ignoreUnknownFields) throws IOException {
        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            return DecisionQuestions.parse(parser, ignoreUnknownFields);
        }
    }

    private void assertParseFailure(String json, String expectedMessage) {
        var exception = expectThrows(IllegalArgumentException.class, () -> parse(json, false));
        var messages = new StringBuilder();
        for (Throwable t = exception; t != null; t = t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        assertThat(messages.toString(), containsString(expectedMessage));
    }

    @Override
    protected DecisionQuestions mutateInstanceForVersion(DecisionQuestions instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected DecisionQuestions doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return DecisionQuestions.parse(parser, false);
    }

    @Override
    protected Writeable.Reader<DecisionQuestions> instanceReader() {
        return DecisionQuestions::new;
    }

    @Override
    protected DecisionQuestions createTestInstance() {
        return createRandom();
    }

    public static DecisionQuestions createRandom() {
        var questions = new LinkedHashMap<String, DecisionQuestion>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            questions.put(randomAlphaOfLength(8) + i, randomQuestion());
        }
        return new DecisionQuestions(questions);
    }

    public static DecisionQuestion randomQuestion() {
        var instructions = randomAlphaOfLengthBetween(1, 30);
        return switch (randomFrom(DecisionQuestion.Type.values())) {
            case PREDICATE -> new DecisionQuestion.Predicate(instructions);
            case CHOICE -> {
                var choices = new HashMap<String, String>();
                for (int i = 0; i < randomIntBetween(DecisionQuestion.MIN_CHOICES, 5); i++) {
                    choices.put(randomAlphaOfLength(8) + i, randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null);
                }
                yield new DecisionQuestion.Choice(instructions, choices);
            }
            case SCORE -> {
                var levels = new ArrayList<DecisionQuestion.Level>();
                for (int i = 0; i < randomIntBetween(DecisionQuestion.MIN_LEVELS, 5); i++) {
                    levels.add(
                        new DecisionQuestion.Level(randomAlphaOfLength(8) + i, randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null)
                    );
                }
                yield new DecisionQuestion.Score(instructions, levels);
            }
        };
    }

    @Override
    protected DecisionQuestions mutateInstance(DecisionQuestions instance) throws IOException {
        var questions = new LinkedHashMap<>(instance.questions());
        if (questions.size() > 1 && randomBoolean()) {
            questions.remove(randomFrom(questions.keySet()));
        } else {
            questions.put(randomAlphaOfLength(10), randomQuestion());
        }
        return new DecisionQuestions(questions);
    }
}
