/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class SuggestProfilesRequestTests extends ESTestCase {

    public void testValidation() {
        final SuggestProfilesRequest request1 = new SuggestProfilesRequest(randomDataKeys(), randomName(), randomSize(), randomHint());
        assertThat(request1.validate(), nullValue());
    }

    public void testValidationWillNotAllowNegativeSize() {
        final SuggestProfilesRequest request1 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomIntBetween(Integer.MIN_VALUE, -1),
            randomHint()
        );
        assertThat(request1.validate().getMessage(), containsString("[size] parameter cannot be negative"));
    }

    public void testValidationWillNotAllowEmptyHints() {
        final SuggestProfilesRequest request1 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomSize(),
            new SuggestProfilesRequest.Hint(null, null)
        );
        assertThat(request1.validate().getMessage(), containsString("[hint] parameter cannot be empty"));

        final SuggestProfilesRequest request2 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomSize(),
            new SuggestProfilesRequest.Hint(List.of(), null)
        );
        assertThat(request2.validate().getMessage(), containsString("[uids] hint cannot be empty"));
    }

    public void testValidationLabels() {
        final SuggestProfilesRequest request1 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomSize(),
            new SuggestProfilesRequest.Hint(
                null,
                randomFrom(Map.of(), randomMap(2, 5, () -> new Tuple<>(randomAlphaOfLength(20), randomAlphaOfLengthBetween(3, 8))))
            )
        );
        assertThat(request1.validate().getMessage(), containsString("[labels] hint supports a single key"));

        final SuggestProfilesRequest request2 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomSize(),
            new SuggestProfilesRequest.Hint(
                null,
                Map.of(randomFrom("*", "a*", "*b", "a*b"), randomList(1, 5, () -> randomAlphaOfLengthBetween(3, 8)))
            )
        );
        assertThat(request2.validate().getMessage(), containsString("[labels] hint key cannot contain wildcard"));

        final SuggestProfilesRequest request3 = new SuggestProfilesRequest(
            randomDataKeys(),
            randomName(),
            randomSize(),
            new SuggestProfilesRequest.Hint(null, Map.of(randomAlphaOfLength(5), List.of()))
        );
        assertThat(request3.validate().getMessage(), containsString("[labels] hint value cannot be empty"));
    }

    public void testErrorOnHintInstantiation() {

        final ElasticsearchParseException e1 = expectThrows(
            ElasticsearchParseException.class,
            () -> new SuggestProfilesRequest.Hint(
                null,
                Map.of(randomAlphaOfLength(5), randomFrom(0, 42.0, randomBoolean(), Map.of(randomAlphaOfLength(5), randomAlphaOfLength(5))))
            )
        );
        assertThat(e1.getMessage(), containsString("[labels] hint supports either string or list of strings as its value"));

        final ElasticsearchParseException e2 = expectThrows(
            ElasticsearchParseException.class,
            () -> new SuggestProfilesRequest.Hint(null, Map.of(randomAlphaOfLength(5), List.of(0, randomAlphaOfLength(8))))
        );
        assertThat(e2.getMessage(), containsString("[labels] hint supports either string value or list of strings"));
    }

    private int randomSize() {
        return randomIntBetween(0, Integer.MAX_VALUE);
    }

    private Set<String> randomDataKeys() {
        return Set.copyOf(randomList(0, 5, () -> randomAlphaOfLengthBetween(3, 8)));
    }

    private String randomName() {
        return randomAlphaOfLengthBetween(0, 8);
    }

    public static SuggestProfilesRequest.Hint randomHint() {
        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                return new SuggestProfilesRequest.Hint(randomList(1, 5, () -> randomAlphaOfLength(20)), null);
            }
            case 1 -> {
                return new SuggestProfilesRequest.Hint(
                    null,
                    Map.of(
                        randomAlphaOfLengthBetween(3, 8),
                        randomFrom(randomAlphaOfLengthBetween(3, 8), randomList(1, 5, () -> randomAlphaOfLengthBetween(3, 8)))
                    )
                );
            }
            case 2 -> {
                return new SuggestProfilesRequest.Hint(
                    randomList(1, 5, () -> randomAlphaOfLength(20)),
                    Map.of(
                        randomAlphaOfLengthBetween(3, 8),
                        randomFrom(randomAlphaOfLengthBetween(3, 8), randomList(1, 5, () -> randomAlphaOfLengthBetween(3, 8)))
                    )
                );
            }
            default -> {
                return null;
            }
        }
    }
}
