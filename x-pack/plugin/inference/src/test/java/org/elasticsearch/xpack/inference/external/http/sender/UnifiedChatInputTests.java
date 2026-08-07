/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.List;

public class UnifiedChatInputTests extends ESTestCase {

    public void testConvertsStringInputToMessages() {
        var a = new UnifiedChatInput(List.of("hello", "awesome"), "a role", true);

        assertThat(a.isSingleInput(), Matchers.is(false));
        assertThat(
            a.getRequest(),
            Matchers.is(
                UnifiedCompletionRequest.of(
                    List.of(
                        new Message(new ContentString("hello"), "a role", null, null),
                        new Message(new ContentString("awesome"), "a role", null, null)
                    )
                )
            )
        );
    }
}
