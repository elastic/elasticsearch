/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction.Request;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class PutTrainedModelAliasActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    private static final Set<String> INVALID_CHARACTERS = Set.of(
        "!",
        "@",
        "#",
        "$",
        "%",
        "^",
        "&",
        "*",
        "(",
        ")",
        "+",
        "=",
        "[",
        "]",
        "{",
        "}",
        "|",
        ";",
        ":",
        "'",
        "\"",
        ",",
        "<",
        ">",
        "/",
        "?"
    );

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(10), randomAlphaOfLength(10), randomBoolean());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String modelAlias = instance.getModelAlias();
        String modelId = instance.getModelId();
        boolean reassign = instance.isReassign();
        int value = randomInt(2);
        return switch (value) {
            case 0 -> new Request(randomValueOtherThan(modelAlias, () -> randomAlphaOfLength(10)), modelId, reassign);
            case 1 -> new Request(modelAlias, randomValueOtherThan(modelId, () -> randomAlphaOfLength(10)), reassign);
            case 2 -> new Request(modelAlias, modelId, reassign == false);
            default -> throw new IllegalStateException("Unexpected value " + value);
        };
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testConstructor() {
        expectThrows(Exception.class, () -> new Request(null, randomAlphaOfLength(10), randomBoolean()));
        expectThrows(Exception.class, () -> new Request(randomAlphaOfLength(10), null, randomBoolean()));
    }

    public void testValidate() {
        List<String> validAliases = List.of("a", "1", "2b", "c-3d", "e_4f", "g.5h");
        for (String modelAlias : validAliases) {
            ActionRequestValidationException ex = new Request(modelAlias, "foo", randomBoolean()).validate();
            assertThat("For alias [" + modelAlias + "]", ex, nullValue());
        }
    }

    public void testValidate_modelAliasEqualToModelId() {
        ActionRequestValidationException ex = new Request("foo", "foo", randomBoolean()).validate();
        assertThat(ex, not(nullValue()));
        assertThat(ex.getMessage(), containsString("model_alias [foo] cannot equal model_id [foo]"));
    }

    public void testValidate_modelAliasEqualToModelIdWithInvalidCharacter() {
        String modelAlias = "foo" + randomFrom(INVALID_CHARACTERS);
        ActionRequestValidationException ex = new Request(modelAlias, modelAlias, randomBoolean()).validate();
        assertThat("For alias [" + modelAlias + "]", ex, not(nullValue()));
        assertThat(ex.validationErrors(), hasSize(2));
        assertThat(ex.getMessage(), containsString("model_alias [" + modelAlias + "] cannot equal model_id [" + modelAlias + "]"));
        assertThat(
            ex.getMessage(),
            containsString(
                "can contain lowercase alphanumeric (a-z and 0-9), hyphens or underscores; "
                    + "must start with alphanumeric and cannot end with numbers"
            )
        );
    }

    public void testValidate_modelAliasContainsUppercase() {
        List<String> invalidAliases = List.of("Start", "midDle", "enD");
        for (String invalidAlias : invalidAliases) {
            assertInvalidAlias(invalidAlias);
        }
    }

    public void testValidate_modelAliasEndsWithNumber() {
        String modelAlias = (randomAlphaOfLength(10) + randomIntBetween(0, Integer.MAX_VALUE)).toLowerCase(Locale.ROOT);
        assertInvalidAlias(modelAlias);
    }

    public void testValidate_modelAliasStartsWithInvalidCharacter() {
        Set<String> invalidFirstCharacters = new HashSet<>(INVALID_CHARACTERS);
        // '-', '_' and '.' are not valid as the first character
        invalidFirstCharacters.addAll(Set.of("-", "_", "."));
        String modelAlias = (randomFrom(invalidFirstCharacters) + randomAlphaOfLength(10)).toLowerCase(Locale.ROOT);
        assertInvalidAlias(modelAlias);
    }

    public void testValidate_modelAliasContainsInvalidCharacter() {
        String modelAlias = (randomAlphaOfLength(5) + randomFrom(INVALID_CHARACTERS) + randomAlphaOfLength(5)).toLowerCase(Locale.ROOT);
        assertInvalidAlias(modelAlias);
    }

    public void testValidate_modelAliasEndsWithInvalidCharacter() {
        Set<String> invalidLastCharacters = new HashSet<>(INVALID_CHARACTERS);
        // '-', '_' and '.' are not valid as the last character
        invalidLastCharacters.addAll(Set.of("-", "_", "."));
        String modelAlias = (randomAlphaOfLength(10) + randomFrom(invalidLastCharacters)).toLowerCase(Locale.ROOT);
        assertInvalidAlias(modelAlias);
    }

    private static void assertInvalidAlias(String modelAlias) {
        ActionRequestValidationException ex = new Request(modelAlias, "foo", randomBoolean()).validate();
        assertThat("For alias [" + modelAlias + "]", ex, not(nullValue()));
        assertThat(
            ex.getMessage(),
            containsString(
                "can contain lowercase alphanumeric (a-z and 0-9), hyphens or underscores; "
                    + "must start with alphanumeric and cannot end with numbers"
            )
        );
    }
}
