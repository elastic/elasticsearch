/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class InvalidateApiKeyResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        List<String> invalidatedApiKeys = Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5)));
        List<String> previouslyInvalidatedApiKeys = Arrays.asList(randomArray(2, 3, String[]::new, () -> randomAlphaOfLength(5)));
        List<ElasticsearchException> errors = Arrays.asList(
            randomArray(
                2,
                5,
                ElasticsearchException[]::new,
                () -> new ElasticsearchException(randomAlphaOfLength(5), new IllegalArgumentException(randomAlphaOfLength(4)))
            )
        );

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject()
            .array("invalidated_api_keys", invalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
            .array("previously_invalidated_api_keys", previouslyInvalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
            .field("error_count", errors.size());
        if (errors.isEmpty() == false) {
            builder.field("error_details");
            builder.startArray();
            for (ElasticsearchException e : errors) {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, e);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final InvalidateApiKeyResponse response = InvalidateApiKeyResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getInvalidatedApiKeys(), containsInAnyOrder(invalidatedApiKeys.toArray(Strings.EMPTY_ARRAY)));
        assertThat(
            response.getPreviouslyInvalidatedApiKeys(),
            containsInAnyOrder(previouslyInvalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
        );
        assertThat(response.getErrors(), is(notNullValue()));
        assertThat(response.getErrors().size(), is(errors.size()));
        assertThat(response.getErrors().get(0).toString(), containsString("type=illegal_argument_exception"));
        assertThat(response.getErrors().get(1).toString(), containsString("type=illegal_argument_exception"));
    }

    public void testEqualsHashCode() {
        List<String> invalidatedApiKeys = Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5)));
        List<String> previouslyInvalidatedApiKeys = Arrays.asList(randomArray(2, 3, String[]::new, () -> randomAlphaOfLength(5)));
        List<ElasticsearchException> errors = Arrays.asList(
            randomArray(
                2,
                5,
                ElasticsearchException[]::new,
                () -> new ElasticsearchException(randomAlphaOfLength(5), new IllegalArgumentException(randomAlphaOfLength(4)))
            )
        );
        InvalidateApiKeyResponse invalidateApiKeyResponse = new InvalidateApiKeyResponse(
            invalidatedApiKeys,
            previouslyInvalidatedApiKeys,
            errors
        );

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(invalidateApiKeyResponse, (original) -> {
            return new InvalidateApiKeyResponse(
                original.getInvalidatedApiKeys(),
                original.getPreviouslyInvalidatedApiKeys(),
                original.getErrors()
            );
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(invalidateApiKeyResponse, (original) -> {
            return new InvalidateApiKeyResponse(
                original.getInvalidatedApiKeys(),
                original.getPreviouslyInvalidatedApiKeys(),
                original.getErrors()
            );
        }, InvalidateApiKeyResponseTests::mutateTestItem);
    }

    private static InvalidateApiKeyResponse mutateTestItem(InvalidateApiKeyResponse original) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new InvalidateApiKeyResponse(
                    Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5))),
                    original.getPreviouslyInvalidatedApiKeys(),
                    original.getErrors()
                );
            case 1:
                return new InvalidateApiKeyResponse(original.getInvalidatedApiKeys(), Collections.emptyList(), original.getErrors());
            case 2:
                return new InvalidateApiKeyResponse(
                    original.getInvalidatedApiKeys(),
                    original.getPreviouslyInvalidatedApiKeys(),
                    Collections.emptyList()
                );
            default:
                return new InvalidateApiKeyResponse(
                    Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5))),
                    original.getPreviouslyInvalidatedApiKeys(),
                    original.getErrors()
                );
        }
    }
}
