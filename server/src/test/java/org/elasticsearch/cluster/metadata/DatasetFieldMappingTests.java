/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

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
        return DatasetFieldMapping.withFormat(
            randomFrom("keyword", "long", "integer", "double", "boolean", "date"),
            randomBoolean() ? null : randomAlphaOfLength(6).toLowerCase(Locale.ROOT),
            randomFormat()
        );
    }

    // This is the shape-only model layer, so it round-trips `format` as an opaque optional string regardless of type;
    // the type-must-be-date and pattern-validity checks live in the ES|QL DeclaredSchemaValidator.
    private static String randomFormat() {
        return randomBoolean() ? null : randomFrom("epoch_millis", "yyyy-MM-dd", "dd/MMM/yyyy:HH:mm:ss Z", "strict_date_optional_time");
    }

    @Override
    protected DatasetFieldMapping mutateInstance(DatasetFieldMapping instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> DatasetFieldMapping.withFormat(
                randomValueOtherThan(instance.type(), () -> randomFrom("keyword", "long", "integer", "double", "boolean", "date")),
                instance.renameFrom(),
                instance.format()
            );
            case 1 -> DatasetFieldMapping.withFormat(
                instance.type(),
                randomValueOtherThan(instance.renameFrom(), () -> randomBoolean() ? null : randomAlphaOfLength(7).toLowerCase(Locale.ROOT)),
                instance.format()
            );
            default -> DatasetFieldMapping.withFormat(
                instance.type(),
                instance.renameFrom(),
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
     * A declared field deliberately supports only {@code type}, {@code rename_from} (the file column this column is
     * renamed from, formerly spelled {@code path}), and {@code format} (the date parse-pattern, on {@code date}
     * columns). Every other core field-mapper parameter
     * must be rejected at parse time. This guards against silently diverging from the
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

    /** The current spelling of the rename key. */
    public void testRenameFromIsParsed() throws IOException {
        String json = "{\"type\":\"date\",\"rename_from\":\"event_time\",\"format\":\"yyyy-MM-dd\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DatasetFieldMapping mapping = DatasetFieldMapping.fromXContent(parser);
            assertEquals("date", mapping.type());
            assertEquals("event_time", mapping.renameFrom());
            assertEquals("yyyy-MM-dd", mapping.format());
        }
    }

    /**
     * {@code path} is the former spelling and stays accepted on equal terms — no warning — so a dataset registered
     * before the rename keeps working, whether its definition arrives in a request or is read back from cluster state.
     */
    public void testPathIsStillAccepted() throws IOException {
        String json = "{\"type\":\"date\",\"path\":\"event_time\",\"format\":\"yyyy-MM-dd\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DatasetFieldMapping mapping = DatasetFieldMapping.fromXContent(parser);
            assertEquals("event_time", mapping.renameFrom());
            assertEquals("yyyy-MM-dd", mapping.format());
        }
    }

    /** Either spelling parses to the same value, so the older one is not a second behaviour. */
    public void testBothSpellingsParseEqual() throws IOException {
        DatasetFieldMapping viaNew;
        DatasetFieldMapping viaOld;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"type\":\"long\",\"rename_from\":\"c0\"}")) {
            viaNew = DatasetFieldMapping.fromXContent(parser);
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"type\":\"long\",\"path\":\"c0\"}")) {
            viaOld = DatasetFieldMapping.fromXContent(parser);
        }
        assertEquals(viaNew, viaOld);
    }

    /** A definition stored under the old spelling reads back under the current one. */
    public void testPathIsEmittedAsRenameFrom() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"type\":\"long\",\"path\":\"c0\"}")) {
            DatasetFieldMapping mapping = DatasetFieldMapping.fromXContent(parser);
            String emitted = Strings.toString(mapping);
            assertThat(emitted, containsString("rename_from"));
            assertThat(emitted, not(containsString("\"path\"")));
        }
    }

    /** Both spellings in one entry is a contradiction with no defined winner, so it is refused at the boundary. */
    public void testBothSpellingsInOneEntryIsRejected() throws IOException {
        String json = "{\"type\":\"long\",\"rename_from\":\"a\",\"path\":\"b\",\"format\":\"yyyy-MM-dd\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Exception e = expectThrows(Exception.class, () -> DatasetFieldMapping.fromXContent(parser));
            assertThat(ExceptionsHelper.stackTrace(e), containsString("not both"));
        }
    }

    /**
     * A mapping declared under the old spelling survives the wire unchanged. This does NOT prove the rename needs no
     * transport version — {@code copyInstance} serializes at {@link org.elasticsearch.TransportVersion#current()} on
     * both ends, so it would pass even if a gate were required. That claim rests on the released stream form being
     * positional and unchanged: three slots, same order, on either side of the rename.
     */
    public void testPathDeclaredMappingRoundTripsOverTheWire() throws IOException {
        DatasetFieldMapping viaOld;
        try (
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"type\":\"date\",\"path\":\"ts\",\"format\":\"yyyy-MM-dd\"}")
        ) {
            viaOld = DatasetFieldMapping.fromXContent(parser);
        }
        DatasetFieldMapping copied = copyInstance(viaOld);
        assertEquals(viaOld, copied);
        assertEquals("ts", copied.renameFrom());
        assertEquals("yyyy-MM-dd", copied.format());
    }
}
