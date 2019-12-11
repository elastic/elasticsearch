/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

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
        List<ElasticsearchException> errors = Arrays.asList(randomArray(2, 5, ElasticsearchException[]::new,
                () -> new ElasticsearchException(randomAlphaOfLength(5), new IllegalArgumentException(randomAlphaOfLength(4)))));

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject().array("invalidated_api_keys", invalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
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
        assertThat(response.getPreviouslyInvalidatedApiKeys(),
                containsInAnyOrder(previouslyInvalidatedApiKeys.toArray(Strings.EMPTY_ARRAY)));
        assertThat(response.getErrors(), is(notNullValue()));
        assertThat(response.getErrors().size(), is(errors.size()));
        assertThat(response.getErrors().get(0).getCause().toString(), containsString("type=illegal_argument_exception"));
        assertThat(response.getErrors().get(1).getCause().toString(), containsString("type=illegal_argument_exception"));
    }

    public void testEqualsHashCode() {
        List<String> invalidatedApiKeys = Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5)));
        List<String> previouslyInvalidatedApiKeys = Arrays.asList(randomArray(2, 3, String[]::new, () -> randomAlphaOfLength(5)));
        List<ElasticsearchException> errors = Arrays.asList(randomArray(2, 5, ElasticsearchException[]::new,
                () -> new ElasticsearchException(randomAlphaOfLength(5), new IllegalArgumentException(randomAlphaOfLength(4)))));
        InvalidateApiKeyResponse invalidateApiKeyResponse = new InvalidateApiKeyResponse(invalidatedApiKeys, previouslyInvalidatedApiKeys,
                errors);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(invalidateApiKeyResponse, (original) -> {
            return new InvalidateApiKeyResponse(original.getInvalidatedApiKeys(), original.getPreviouslyInvalidatedApiKeys(),
                    original.getErrors());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(invalidateApiKeyResponse, (original) -> {
            return new InvalidateApiKeyResponse(original.getInvalidatedApiKeys(), original.getPreviouslyInvalidatedApiKeys(),
                    original.getErrors());
        }, InvalidateApiKeyResponseTests::mutateTestItem);
    }

    private static InvalidateApiKeyResponse mutateTestItem(InvalidateApiKeyResponse original) {
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new InvalidateApiKeyResponse(Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5))),
                    original.getPreviouslyInvalidatedApiKeys(), original.getErrors());
        case 1:
            return new InvalidateApiKeyResponse(original.getInvalidatedApiKeys(), Collections.emptyList(), original.getErrors());
        case 2:
            return new InvalidateApiKeyResponse(original.getInvalidatedApiKeys(), original.getPreviouslyInvalidatedApiKeys(),
                    Collections.emptyList());
        default:
            return new InvalidateApiKeyResponse(Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLength(5))),
                    original.getPreviouslyInvalidatedApiKeys(), original.getErrors());
        }
    }
}
