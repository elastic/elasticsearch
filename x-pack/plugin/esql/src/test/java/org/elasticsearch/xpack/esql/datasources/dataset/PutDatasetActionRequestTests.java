/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction.Request;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
            randomSettings(),
            randomMappingOrNull()
        );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return switch (between(0, 5)) {
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
                    mutated,
                    instance.mapping()
                );
            }
            case 5 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                instance.rawSettings(),
                randomValueOtherThan(instance.mapping(), PutDatasetActionRequestTests::randomMappingOrNull)
            );
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

    static DatasetMapping randomMappingOrNull() {
        if (randomBoolean()) {
            return null;
        }
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        int n = between(0, 3);
        for (int i = 0; i < n; i++) {
            props.put(
                "c" + i,
                new DatasetFieldMapping(
                    randomFrom("keyword", "long", "integer", "double", "boolean", "date"),
                    randomBoolean() ? null : randomAlphaOfLength(4).toLowerCase(Locale.ROOT)
                )
            );
        }
        String idPath = randomBoolean() ? null : randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        return DatasetMapping.assemble(new DatasetMapping.Mappings(randomFrom(DatasetMapping.Dynamic.values()), props, idPath));
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
