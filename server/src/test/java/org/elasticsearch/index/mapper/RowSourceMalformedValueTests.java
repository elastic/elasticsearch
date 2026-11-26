/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Parses the same documents through {@code DocumentParser} from a {@link BytesSource}, which is read with a JSON parser, and from a
 * {@link RowSource}, which is read with {@link SourceRowXContentParser}, for fields that have {@code ignore_malformed}, and requires the
 * same outcome from both: the same fields indexed and ignored when the document is accepted, and the same parse error, leaving aside the
 * position, when it is not. The parser tests check each accessor of {@link SourceRowXContentParser} against a JSON parser; this checks the
 * behavior they exist for, which is what a mapper does when it is given a value it cannot use.
 */
public class RowSourceMalformedValueTests extends MapperServiceTestCase {

    /** What parsing a document came to: the fields it indexed and ignored, or the error it failed with. */
    private record Outcome(List<String> ignored, List<String> indexed, String error) {}

    private DocumentMapper documentMapper() throws IOException {
        return createDocumentMapper(mapping(b -> {
            field(b, "int_field", "integer");
            field(b, "short_field", "short");
            field(b, "long_field", "long");
            field(b, "double_field", "double");
            field(b, "float_field", "float");
            field(b, "bool_field", "boolean");
            b.startObject("loc").field("type", "geo_point").endObject();
        }));
    }

    private static void field(XContentBuilder b, String name, String type) throws IOException {
        b.startObject(name).field("type", type).field("ignore_malformed", true).endObject();
    }

    public void testMalformedValuesAreIgnoredAsTheyAreFromJson() throws IOException {
        final DocumentMapper mapper = documentMapper();
        // A boolean where a number is expected.
        assertIgnored(mapper, "{\"int_field\": false, \"double_field\": true}", "double_field", "int_field");
        // A number too big for the field, which a plain cast would have wrapped around.
        assertIgnored(mapper, "{\"int_field\": 3000000000}", "int_field");
        assertIgnored(mapper, "{\"short_field\": 70000}", "short_field");
        assertIgnored(mapper, "{\"short_field\": 32768}", "short_field");
        assertIgnored(mapper, "{\"float_field\": 1e300}", "float_field");
        // Text where a number or a boolean is expected.
        assertIgnored(mapper, "{\"double_field\": \"abc\"}", "double_field");
        assertIgnored(mapper, "{\"bool_field\": \"garbage\"}", "bool_field");
        // A number where a boolean is expected.
        assertIgnored(mapper, "{\"bool_field\": 5}", "bool_field");
    }

    public void testValidValuesAreIndexedAsTheyAreFromJson() throws IOException {
        final DocumentMapper mapper = documentMapper();
        assertIgnored(mapper, "{\"int_field\": 5, \"short_field\": 7, \"long_field\": 9, \"double_field\": 1.5, \"bool_field\": true}");
        assertIgnored(mapper, "{\"int_field\": 2147483647, \"short_field\": -32768, \"long_field\": 9223372036854775807}");
        assertIgnored(mapper, "{\"int_field\": [1, 2, 3]}");
    }

    /**
     * A value that is not a scalar cannot be ignored, because {@code ignore_malformed} only applies to a value. The document is rejected,
     * with the same error as from JSON, and in particular a boolean field is not quietly given an object's worth of {@code false}.
     */
    public void testValuesThatCannotBeIgnoredAreRejectedAsTheyAreFromJson() throws IOException {
        final DocumentMapper mapper = documentMapper();
        assertRejected(mapper, "{\"bool_field\": {\"object\": \"wow\"}}", "failed to parse field [bool_field] of type [boolean]");
        assertRejected(mapper, "{\"int_field\": {\"object\": \"wow\"}}", "failed to parse field [int_field] of type [integer]");
        // An object as an element of an array, where the mapper is handed the start of the object.
        assertRejected(mapper, "{\"int_field\": [7, {\"x\": 1}]}", "failed to parse field [int_field] of type [integer]");
        assertRejected(mapper, "{\"bool_field\": [true, {\"x\": 1}]}", "failed to parse field [bool_field] of type [boolean]");
    }

    /**
     * The array form of a geo_point reads its two coordinates in turn, and does not check what token follows the first. A token that is
     * not a number must make the document fail, as it does from JSON, not be read as a copy of the first coordinate, which indexed
     * {@code [1.5, [2.5]]} at (1.5, 1.5).
     */
    public void testGeoPointArrayWithoutTwoNumbersIsRejectedAsItIsFromJson() throws IOException {
        final DocumentMapper mapper = documentMapper();
        assertIgnored(mapper, "{\"loc\": [1.5, 2.5]}");
        assertRejectedWithTheSameFirstLine(mapper, "{\"loc\": [1.5, [2.5]]}");
        assertRejectedWithTheSameFirstLine(mapper, "{\"loc\": [1.5, {\"a\": 1}]}");
        assertRejectedWithTheSameFirstLine(mapper, "{\"loc\": [1.5, \"x\"]}");
        assertRejectedWithTheSameFirstLine(mapper, "{\"loc\": [1.5]}");
    }

    /**
     * For a mapper that reports what the parser said rather than its own message. The JSON parser's message goes on to quote the source
     * and where in it the error was, which a row has no text to say, so only the first line, without its position, is compared.
     */
    private void assertRejectedWithTheSameFirstLine(DocumentMapper mapper, String json) throws IOException {
        final Outcome fromJson = parseFromJson(mapper, json);
        assertThat("from JSON: " + json, fromJson.error(), notNullValue());
        final Outcome fromRow = parseFromRow(mapper, json);
        assertThat("the row source of " + json, fromRow.error(), notNullValue());
        assertThat("the row source of " + json, firstLine(fromRow.error()), equalTo(firstLine(fromJson.error())));
    }

    private static String firstLine(String message) {
        final int end = message.indexOf('\n');
        // Leave out the position, which a row has no text to say.
        return (end < 0 ? message : message.substring(0, end)).replaceAll("\\[\\d+:\\d+\\] ", "");
    }

    private void assertIgnored(DocumentMapper mapper, String json, String... ignoredFields) throws IOException {
        final Outcome fromJson = parseFromJson(mapper, json);
        assertThat("from JSON: " + json, fromJson.error(), nullValue());
        assertThat("ignored fields of " + json, fromJson.ignored(), equalTo(List.of(ignoredFields)));
        assertThat("the row source of " + json, parseFromRow(mapper, json), equalTo(fromJson));
    }

    private void assertRejected(DocumentMapper mapper, String json, String expectedError) throws IOException {
        final Outcome fromJson = parseFromJson(mapper, json);
        assertThat("from JSON: " + json, fromJson.error(), containsString(expectedError));
        assertThat("the row source of " + json, parseFromRow(mapper, json), equalTo(fromJson));
    }

    private Outcome parseFromJson(DocumentMapper mapper, String json) {
        return parse(mapper, new BytesSource(new BytesArray(json), XContentType.JSON, true));
    }

    private Outcome parseFromRow(DocumentMapper mapper, String json) throws IOException {
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON)) {
            final RowSource row = new RowSource(SourceRowXContentParser.buildSchemaTree(batch.schema()), batch.row(0), XContentType.JSON);
            return parse(mapper, row);
        }
    }

    private static Outcome parse(DocumentMapper mapper, DocumentSource source) {
        try {
            final ParsedDocument doc = mapper.parse(
                new SourceToParse("1", source, null, Map.of(), Map.of(), XContentMeteringParserDecorator.NOOP, null)
            );
            final List<String> ignored = new ArrayList<>();
            final List<String> indexed = new ArrayList<>();
            for (IndexableField field : doc.rootDoc().getFields()) {
                if (field.name().equals(IgnoredFieldMapper.NAME)) {
                    if (field.stringValue() != null) {
                        ignored.add(field.stringValue());
                    }
                } else if (field.name().startsWith("_") == false) {
                    indexed.add(field.name() + "=" + valueOf(field));
                }
            }
            Collections.sort(ignored);
            Collections.sort(indexed);
            return new Outcome(ignored, indexed, null);
        } catch (Exception e) {
            // The position of the error is where the parser was, which a row has no text to say.
            return new Outcome(null, null, e.getClass().getSimpleName() + ": " + e.getMessage().replaceFirst("^\\[\\d+:\\d+\\] ", ""));
        }
    }

    private static Object valueOf(IndexableField field) {
        if (field.numericValue() != null) {
            return field.numericValue();
        }
        if (field.stringValue() != null) {
            return field.stringValue();
        }
        return field.binaryValue();
    }
}
