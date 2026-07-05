/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction.Request;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Wire round-trip + validate() tests for {@link Request}. */
public class PutDatasetActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomName(),
            randomName(),
            "s3://" + randomAlphaOfLength(6) + "/" + randomAlphaOfLength(4) + "/*.parquet",
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
        return switch (between(0, 4)) {
            case 0 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name() + "_mutated",
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                instance.rawSettings()
            );
            case 1 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource() + "_mutated",
                instance.resource(),
                instance.description(),
                instance.rawSettings()
            );
            case 2 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource(),
                instance.resource() + "/extra",
                instance.description(),
                instance.rawSettings()
            );
            case 3 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                randomValueOtherThan(instance.description(), () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)),
                instance.rawSettings()
            );
            case 4 -> {
                Map<String, Object> mutated = new HashMap<>(instance.rawSettings());
                mutated.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
                yield new Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    instance.name(),
                    instance.dataSource(),
                    instance.resource(),
                    instance.description(),
                    mutated
                );
            }
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testValidateAcceptsCleanRequest() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "parent", "s3://bucket/path", null, Map.of());
        assertThat(r.validate(), nullValue());
    }

    public void testValidateRejectsEmptyName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "", "parent", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset name is missing"));
    }

    public void testValidateRejectsUppercaseName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "MyDS", "parent", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("must be lowercase"));
    }

    public void testValidateRejectsEmptyDataSource() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset data_source is missing"));
    }

    public void testValidateRejectsEmptyResource() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "parent", "", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset resource is missing"));
    }

    public void testValidateRejectsWhitespaceOnlyName() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "   ", "parent", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset name is missing"));
    }

    public void testValidateRejectsNameWithInvalidCharacters() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "bad#name", "parent", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("invalid dataset name"));
    }

    public void testValidateRejectsNameStartingWithUnderscore() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "_leading", "parent", "s3://bucket", null, Map.of());
        ActionRequestValidationException v = r.validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("invalid dataset name"));
    }

    public void testValidateAcceptsAbsentSettingsAsEmpty() {
        // Absent optional container ≡ empty — a normal client shape for datasets inheriting from the parent.
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds", "parent", "s3://bucket", null, null);
        assertThat(r.validate(), nullValue());
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
