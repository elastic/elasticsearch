/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction.Request;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Wire round-trip + validate() tests for {@link Request}. */
public class PutDataSourceActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomName(),
            randomFrom("s3", "gcs", "azure", "test"),
            randomBoolean() ? null : randomAlphaOfLengthBetween(0, 20),
            randomSettings()
        );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return switch (between(0, 3)) {
            case 0 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name() + "_mutated",
                instance.type(),
                instance.description(),
                instance.rawSettings()
            );
            case 1 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                randomValueOtherThan(instance.type(), () -> randomFrom("s3", "gcs", "azure", "test")),
                instance.description(),
                instance.rawSettings()
            );
            case 2 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.type(),
                randomValueOtherThan(instance.description(), () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)),
                instance.rawSettings()
            );
            case 3 -> {
                Map<String, Object> mutated = new HashMap<>(instance.rawSettings());
                mutated.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
                yield new Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    instance.name(),
                    instance.type(),
                    instance.description(),
                    mutated
                );
            }
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testValidateAcceptsCleanRequest() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "s3", null, Map.of("region", "us-east-1"));
        assertThat(r.validate(), nullValue());
    }

    public void testValidateRejectsEmptyName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "", "s3", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("data source name is missing"));
    }

    public void testValidateRejectsUppercaseName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "MyDS", "s3", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("must be lowercase"));
    }

    public void testValidateRejectsEmptyType() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("data source type is missing"));
    }

    public void testValidateRejectsWhitespaceOnlyName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "   ", "s3", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("data source name is missing"));
    }

    public void testValidateRejectsNameWithInvalidCharacters() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "bad#name", "s3", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("invalid data source name"));
    }

    public void testValidateRejectsNameStartingWithUnderscore() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "_leading_underscore", "s3", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("invalid data source name"));
    }

    public void testValidateAcceptsAbsentSettingsAsEmpty() {
        // Matches ES-wide precedent: absent optional container ≡ empty map (see CreateIndexRequest
        // Settings.EMPTY default, SLM optionalConstructorArg, inference removeFromMapOrDefaultEmpty).
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "s3", null, null);
        assertThat(r.validate(), nullValue());
        // Constructor defaults null rawSettings to Map.of() — downstream reads see an empty map.
        assertThat(r.rawSettings(), notNullValue());
    }

    private static String randomName() {
        return randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
    }

    private static Map<String, Object> randomSettings() {
        Map<String, Object> out = new HashMap<>();
        int count = between(0, 5);
        for (int i = 0; i < count; i++) {
            out.put(randomAlphaOfLength(6), randomAlphaOfLength(8));
        }
        return out;
    }
}
