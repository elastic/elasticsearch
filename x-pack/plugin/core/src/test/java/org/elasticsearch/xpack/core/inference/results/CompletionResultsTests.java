/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CompletionResultsTests extends AbstractWireSerializingTestCase<CompletionResults> {

    public void testToXContent_CreateTheRightFormatForASingleChatCompletionResult() {
        String resultContent = "content";
        var result = new CompletionResults(List.of(new CompletionResults.Result(resultContent)));

        assertThat(
            result.asMap(),
            is(Map.of(CompletionResults.COMPLETION, List.of(Map.of(CompletionResults.Result.RESULT, resultContent))))
        );

        String xContentResult = Strings.toTruncatedString(result, true, true);
        assertThat(xContentResult, is("""
            {
              "completion" : [
                {
                  "result" : "content"
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleCompletionResults() {
        String resultOneContent = "content 1";
        String resultTwoContent = "content 2";

        var entity = new CompletionResults(
            List.of(new CompletionResults.Result(resultOneContent), new CompletionResults.Result(resultTwoContent))
        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    CompletionResults.COMPLETION,
                    List.of(
                        Map.of(CompletionResults.Result.RESULT, resultOneContent),
                        Map.of(CompletionResults.Result.RESULT, resultTwoContent)
                    )
                )
            )
        );

        String xContentResult = Strings.toTruncatedString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "completion" : [
                {
                  "result" : "content 1"
                },
                {
                  "result" : "content 2"
                }
              ]
            }"""));
    }

    public void testTransformToCoordinationFormat() {
        String resultOneContent = "content 1";
        String resultTwoContent = "content 2";

        var entity = new CompletionResults(
            List.of(new CompletionResults.Result(resultOneContent), new CompletionResults.Result(resultTwoContent))
        );

        var transformedEntity = entity.transformToCoordinationFormat();

        assertThat(transformedEntity.get(0).asMap(), is(Map.of(CompletionResults.Result.RESULT, resultOneContent)));
        assertThat(transformedEntity.get(1).asMap(), is(Map.of(CompletionResults.Result.RESULT, resultTwoContent)));
    }

    @Override
    protected Writeable.Reader<CompletionResults> instanceReader() {
        return CompletionResults::new;
    }

    @Override
    protected CompletionResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected CompletionResults mutateInstance(CompletionResults instance) throws IOException {
        // if true we reduce the chat results list by a random amount, if false we add a chat result to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.results().size() - 1);
            return new CompletionResults(instance.results().subList(0, end));
        } else {
            List<CompletionResults.Result> completionResults = new ArrayList<>(instance.results());
            completionResults.add(createRandomChatCompletionResult());
            return new CompletionResults(completionResults);
        }
    }

    public static CompletionResults createRandomResults() {
        int numOfCompletionResults = randomIntBetween(1, 10);
        List<CompletionResults.Result> chatCompletionResults = new ArrayList<>(numOfCompletionResults);

        for (int i = 0; i < numOfCompletionResults; i++) {
            chatCompletionResults.add(createRandomChatCompletionResult());
        }

        return new CompletionResults(chatCompletionResults);
    }

    public static Map<String, Object> buildExpectationCompletion(List<String> results) {
        return Map.of(
            CompletionResults.COMPLETION,
            results.stream().map(result -> Map.of(CompletionResults.Result.RESULT, result)).toList()
        );
    }

    private static CompletionResults.Result createRandomChatCompletionResult() {
        return new CompletionResults.Result(randomAlphaOfLengthBetween(10, 300));
    }
}
