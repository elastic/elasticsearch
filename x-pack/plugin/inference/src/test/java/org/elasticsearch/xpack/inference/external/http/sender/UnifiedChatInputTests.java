/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Template;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;

public class UnifiedChatInputTests extends InferenceObjectRamBytesUsedTest<UnifiedChatInput> {

    private static final List<String> INPUTS = List.of("input");
    private static final String ROLE = "role";

    @Override
    public UnifiedChatInput objectToEstimate() {
        return new UnifiedChatInput(INPUTS, ROLE, false);
    }

    @Override
    public List<UnifiedChatInput> objectsToEstimateWithLargerInput() {
        return List.of(
            // More inputs
            new UnifiedChatInput(List.of(INPUTS.getFirst(), INPUTS.getFirst()), ROLE, false),
            // Larger role
            new UnifiedChatInput(INPUTS, ROLE.repeat(5), false)
        );
    }

    public void testIsSingleInput_TemplateOnlyRequest() {
        var body = new UnifiedCompletionRequestBody(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new Template(".summarize", 1, Map.of())
        );
        var input = new UnifiedChatInput(body, true);

        assertThat(input.isSingleInput(), Matchers.is(true));
        assertThat(input.ramBytesUsed(), Matchers.greaterThan(0L));
    }

    public void testConvertsStringInputToMessages() {
        var a = new UnifiedChatInput(List.of("hello", "awesome"), "a role", true);

        assertThat(a.isSingleInput(), Matchers.is(false));
        assertThat(
            a.getRequest(),
            Matchers.is(
                UnifiedCompletionRequestBody.of(
                    List.of(
                        new Message(new ContentString("hello"), "a role", null, null),
                        new Message(new ContentString("awesome"), "a role", null, null)
                    )
                )
            )
        );
    }
}
