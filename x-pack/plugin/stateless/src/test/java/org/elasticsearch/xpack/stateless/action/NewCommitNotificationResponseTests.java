/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class NewCommitNotificationResponseTests extends ESTestCase {

    public void testCombineResponses() {
        {
            var combinedResponses = NewCommitNotificationResponse.combine(
                List.of(NewCommitNotificationResponse.EMPTY, NewCommitNotificationResponse.EMPTY)
            );
            assertThat(combinedResponses, equalTo(NewCommitNotificationResponse.EMPTY));
        }

        {
            var combinedResponses = NewCommitNotificationResponse.combine(
                List.of(response(new PrimaryTermAndGeneration(1, 2)), NewCommitNotificationResponse.EMPTY)
            );
            assertThat(combinedResponses, equalTo(response(new PrimaryTermAndGeneration(1, 2))));
        }

        {
            var combinedResponses = NewCommitNotificationResponse.combine(
                List.of(NewCommitNotificationResponse.EMPTY, response(new PrimaryTermAndGeneration(1, 2)))
            );
            assertThat(combinedResponses, equalTo(response(new PrimaryTermAndGeneration(1, 2))));
        }

        {
            var combinedResponses = NewCommitNotificationResponse.combine(
                List.of(
                    response(new PrimaryTermAndGeneration(1, 2)),
                    response(new PrimaryTermAndGeneration(1, 2), new PrimaryTermAndGeneration(1, 3))
                )
            );
            assertThat(combinedResponses, equalTo(response(new PrimaryTermAndGeneration(1, 2), new PrimaryTermAndGeneration(1, 3))));
        }

        {
            var combinedResponses = NewCommitNotificationResponse.combine(
                List.of(
                    response(new PrimaryTermAndGeneration(2, 3)),
                    response(new PrimaryTermAndGeneration(1, 2), new PrimaryTermAndGeneration(1, 3))
                )
            );
            assertThat(
                combinedResponses,
                equalTo(
                    response(new PrimaryTermAndGeneration(1, 2), new PrimaryTermAndGeneration(1, 3), new PrimaryTermAndGeneration(2, 3))
                )
            );
        }
    }

    private NewCommitNotificationResponse response(PrimaryTermAndGeneration... generations) {
        return new NewCommitNotificationResponse(Arrays.stream(generations).collect(Collectors.toSet()));
    }
}
