/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.DecisionQuestionsTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDecisionTaskSettingsTests extends AbstractWireSerializingTestCase<
    ElasticInferenceServiceDecisionTaskSettings> {

    private static final Map<String, Object> PREDICATE_QUESTION_MAP = Map.of(
        DecisionQuestion.TYPE_FIELD,
        "predicate",
        DecisionQuestion.INSTRUCTIONS_FIELD,
        "Is the customer complaining?"
    );
    private static final DecisionQuestions PREDICATE_QUESTIONS = new DecisionQuestions(
        Map.of("is_complaint", new DecisionQuestion.Predicate("Is the customer complaining?"))
    );

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDecisionTaskSettings> instanceReader() {
        return ElasticInferenceServiceDecisionTaskSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDecisionTaskSettings createTestInstance() {
        return randomBoolean()
            ? ElasticInferenceServiceDecisionTaskSettings.EMPTY
            : new ElasticInferenceServiceDecisionTaskSettings(DecisionQuestionsTests.createRandom());
    }

    @Override
    protected ElasticInferenceServiceDecisionTaskSettings mutateInstance(ElasticInferenceServiceDecisionTaskSettings instance) {
        return new ElasticInferenceServiceDecisionTaskSettings(
            randomValueOtherThan(instance.questions(), DecisionQuestionsTests::createRandom)
        );
    }

    public void testFromMap_NullOrEmpty_ReturnsEmpty() {
        var context = randomFrom(ConfigurationParseContext.values());

        assertThat(
            ElasticInferenceServiceDecisionTaskSettings.fromMap(null, context),
            is(ElasticInferenceServiceDecisionTaskSettings.EMPTY)
        );
        assertThat(
            ElasticInferenceServiceDecisionTaskSettings.fromMap(Map.of(), context),
            is(ElasticInferenceServiceDecisionTaskSettings.EMPTY)
        );
    }

    public void testFromMap_ParsesQuestions() {
        var taskSettings = ElasticInferenceServiceDecisionTaskSettings.fromMap(
            Map.of(DecisionQuestions.QUESTIONS_FIELD, Map.of("is_complaint", PREDICATE_QUESTION_MAP)),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(taskSettings, is(new ElasticInferenceServiceDecisionTaskSettings(PREDICATE_QUESTIONS)));
        assertFalse(taskSettings.isEmpty());
    }

    public void testFromMap_RejectsUnknownField_RequestContext() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDecisionTaskSettings.fromMap(Map.of("unknown_field", "value"), ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("unknown field [unknown_field]"));
    }

    public void testFromMap_IgnoresUnknownFields_PersistentContext() {
        var questionWithUnknownField = new HashMap<>(PREDICATE_QUESTION_MAP);
        questionWithUnknownField.put("unknown_question_field", "value");

        var taskSettings = ElasticInferenceServiceDecisionTaskSettings.fromMap(
            Map.of(DecisionQuestions.QUESTIONS_FIELD, Map.of("is_complaint", questionWithUnknownField), "unknown_field", "value"),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(taskSettings, is(new ElasticInferenceServiceDecisionTaskSettings(PREDICATE_QUESTIONS)));
    }

    public void testFromMap_RejectsEmptyQuestions() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceDecisionTaskSettings.fromMap(
                Map.of(DecisionQuestions.QUESTIONS_FIELD, Map.of()),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(exception.getCause().getMessage(), containsString("Field [questions] must contain at least one question"));
    }

    public void testUpdatedTaskSettings_EmptyMap_KeepsStoredQuestions() {
        var taskSettings = new ElasticInferenceServiceDecisionTaskSettings(DecisionQuestionsTests.createRandom());

        assertThat(taskSettings.updatedTaskSettings(Map.of()), is(taskSettings));
    }

    public void testUpdatedTaskSettings_ReplacesStoredQuestions() {
        var taskSettings = new ElasticInferenceServiceDecisionTaskSettings(
            new DecisionQuestions(
                Map.of(
                    "anger",
                    new DecisionQuestion.Score(
                        "How angry is the customer?",
                        List.of(new DecisionQuestion.Level("calm"), new DecisionQuestion.Level("furious"))
                    ),
                    "is_complaint",
                    new DecisionQuestion.Predicate("Is this a complaint?")
                )
            )
        );

        var updated = taskSettings.updatedTaskSettings(
            Map.of(DecisionQuestions.QUESTIONS_FIELD, Map.of("is_complaint", PREDICATE_QUESTION_MAP))
        );

        assertThat(updated, is(new ElasticInferenceServiceDecisionTaskSettings(PREDICATE_QUESTIONS)));
    }

    public void testUpdatedTaskSettings_RejectsUnknownField() {
        var taskSettings = ElasticInferenceServiceDecisionTaskSettings.EMPTY;

        expectThrows(XContentParseException.class, () -> taskSettings.updatedTaskSettings(Map.of("unknown_field", "value")));
    }

    public void testToXContent_Empty() {
        assertThat(Strings.toString(ElasticInferenceServiceDecisionTaskSettings.EMPTY), is("{}"));
    }

    public void testToXContent_WithQuestions() throws IOException {
        var taskSettings = new ElasticInferenceServiceDecisionTaskSettings(PREDICATE_QUESTIONS);

        assertThat(Strings.toString(taskSettings), is(XContentHelper.stripWhitespace("""
            {
                "questions": {
                    "is_complaint": {"type": "predicate", "instructions": "Is the customer complaining?"}
                }
            }""")));
    }

    public void testEmptyInstanceIsEmpty() {
        assertTrue(ElasticInferenceServiceDecisionTaskSettings.EMPTY.isEmpty());
        assertThat(ElasticInferenceServiceDecisionTaskSettings.EMPTY.questions(), sameInstance(DecisionQuestions.EMPTY));
    }
}
