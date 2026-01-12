/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.test.ESTestCase;

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
