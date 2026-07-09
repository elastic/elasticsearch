/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class DatasetFieldMappingTests extends AbstractXContentSerializingTestCase<DatasetFieldMapping> {

    @Override
    protected DatasetFieldMapping doParseInstance(XContentParser parser) throws IOException {
        return DatasetFieldMapping.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DatasetFieldMapping> instanceReader() {
        return DatasetFieldMapping::new;
    }

    @Override
    protected DatasetFieldMapping createTestInstance() {
        return new DatasetFieldMapping(
            randomFrom("keyword", "long", "integer", "double", "boolean", "date"),
            randomBoolean() ? null : randomAlphaOfLength(6).toLowerCase(Locale.ROOT),
            randomCopyTo(),
            randomFormat()
        );
    }

    private static List<String> randomCopyTo() {
        int n = randomIntBetween(0, 2);
        List<String> targets = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            targets.add(randomAlphaOfLength(5).toLowerCase(Locale.ROOT));
        }
        return targets;
    }

    // This is the shape-only model layer, so it round-trips `format` as an opaque optional string regardless of type;
    // the type-must-be-date and pattern-validity checks live in the ES|QL DeclaredSchemaValidator.
    private static String randomFormat() {
        return randomBoolean() ? null : randomFrom("epoch_millis", "yyyy-MM-dd", "dd/MMM/yyyy:HH:mm:ss Z", "strict_date_optional_time");
    }

    @Override
    protected DatasetFieldMapping mutateInstance(DatasetFieldMapping instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new DatasetFieldMapping(
                randomValueOtherThan(instance.type(), () -> randomFrom("keyword", "long", "integer", "double", "boolean", "date")),
                instance.path(),
                instance.copyTo(),
                instance.format()
            );
            case 1 -> new DatasetFieldMapping(
                instance.type(),
                randomValueOtherThan(instance.path(), () -> randomBoolean() ? null : randomAlphaOfLength(7).toLowerCase(Locale.ROOT)),
                instance.copyTo(),
                instance.format()
            );
            case 2 -> new DatasetFieldMapping(
                instance.type(),
                instance.path(),
                randomValueOtherThan(instance.copyTo(), DatasetFieldMappingTests::randomCopyTo),
                instance.format()
            );
            default -> new DatasetFieldMapping(
                instance.type(),
                instance.path(),
                instance.copyTo(),
                randomValueOtherThan(instance.format(), DatasetFieldMappingTests::randomFormat)
            );
        };
    }

    public void testTypeRequired() {
        expectThrows(NullPointerException.class, () -> new DatasetFieldMapping(null, "src"));
    }

    /** A declared {@code date} column accepts a {@code format} parse-pattern (parsed here; type/pattern validity is
     *  enforced in the ES|QL DeclaredSchemaValidator, not at parse time). */
    public void testFormatIsParsed() throws IOException {
        String json = "{\"type\":\"date\",\"format\":\"dd/MMM/yyyy:HH:mm:ss Z\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DatasetFieldMapping mapping = DatasetFieldMapping.fromXContent(parser);
            assertEquals("date", mapping.type());
            assertEquals("dd/MMM/yyyy:HH:mm:ss Z", mapping.format());
        }
    }

    /**
     * A declared field deliberately supports only {@code type}, {@code path} (the index {@code alias}-style rename),
     * {@code copy_to} (the index copy), and {@code format} (the date parse-pattern, on {@code date} columns). Every
     * other core field-mapper parameter must be rejected at parse time. This guards against silently diverging from the
     * core mapping: a parameter we don't model can't creep in or be quietly dropped — adding support for one has to be
     * a deliberate change that breaks this test.
     */
    public void testRejectsCoreFieldParametersWeDoNotSupport() throws IOException {
        for (String param : List.of("analyzer", "index", "doc_values", "null_value", "fields", "ignore_above", "store", "norms", "meta")) {
            String json = "{\"type\":\"keyword\",\"" + param + "\":\"x\"}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
                Exception e = expectThrows(Exception.class, () -> DatasetFieldMapping.fromXContent(parser));
                assertThat("core field parameter [" + param + "] must be rejected", e.getMessage(), containsString(param));
            }
        }
    }
}
