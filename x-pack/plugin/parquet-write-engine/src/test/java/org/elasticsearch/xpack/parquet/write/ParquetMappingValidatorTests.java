/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ParquetMappingValidatorTests extends ESTestCase {

    private static final String FORMAT = "[index.storage_format] [parquet]";

    private static void validate(String mapping) throws IOException {
        ParquetMappingValidator.validate(FORMAT, List.of(new CompressedXContent(mapping)), false);
    }

    private static void assertRejected(String mapping, String... messageParts) throws IOException {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(mapping));
        for (String part : messageParts) {
            assertThat(e.getMessage(), containsString(part));
        }
    }

    public void testEverySupportedTypeIsAccepted() throws Exception {
        validate("""
            {"properties": {
              "a": {"type": "long"}, "b": {"type": "integer"}, "c": {"type": "short"}, "d": {"type": "byte"},
              "e": {"type": "date"}, "f": {"type": "keyword"}, "g": {"type": "double"}, "h": {"type": "float"},
              "i": {"type": "boolean"},
              "o": {"properties": {"inner": {"type": "long"}, "deeper": {"type": "object", "properties": {"x": {"type": "keyword"}}}}}
            }}""");
    }

    public void testTheMappingMayBeWrappedInItsType() throws Exception {
        validate("{\"_doc\": {\"properties\": {\"a\": {\"type\": \"long\"}}}}");
        assertRejected("{\"_doc\": {\"properties\": {\"a\": {\"type\": \"text\"}}}}", "type [text]", "field [a]");
    }

    public void testDateNanosIsRefusedBecauseTablesHaveNoNanosecondTimestamp() throws Exception {
        assertRejected("{\"properties\": {\"when\": {\"type\": \"date_nanos\"}}}", "type [date_nanos]", "field [when]", "supported types");
    }

    public void testATypeThatTheEngineDoesNotWriteIsRefusedWithItsFullPath() throws Exception {
        assertRejected("{\"properties\": {\"o\": {\"properties\": {\"ip\": {\"type\": \"ip\"}}}}}", "type [ip]", "field [o.ip]");
        assertRejected("{\"properties\": {\"t\": {\"type\": \"text\"}}}", "type [text]");
        assertRejected("{\"properties\": {\"n\": {\"type\": \"nested\"}}}", "type [nested]");
        assertRejected("{\"properties\": {\"s\": {\"type\": \"scaled_float\", \"scaling_factor\": 100}}}", "type [scaled_float]");
    }

    public void testAnIndexedFieldIsRefused() throws Exception {
        assertRejected("{\"properties\": {\"k\": {\"type\": \"keyword\", \"index\": true}}}", "field [k]", "[index]");
        validate("{\"properties\": {\"k\": {\"type\": \"keyword\", \"index\": false}}}");
    }

    public void testAFieldWithoutDocValuesIsRefused() throws Exception {
        assertRejected("{\"properties\": {\"k\": {\"type\": \"long\", \"doc_values\": false}}}", "field [k]", "doc values");
    }

    public void testMultiFieldsAreRefused() throws Exception {
        assertRejected(
            "{\"properties\": {\"k\": {\"type\": \"keyword\", \"fields\": {\"raw\": {\"type\": \"keyword\"}}}}}",
            "multi-fields",
            "[k]"
        );
    }

    public void testEveryMappingOfTheListIsChecked() throws Exception {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetMappingValidator.validate(
                FORMAT,
                List.of(
                    new CompressedXContent("{\"properties\": {\"a\": {\"type\": \"long\"}}}"),
                    new CompressedXContent("{\"properties\": {\"b\": {\"type\": \"date_nanos\"}}}")
                ),
                false
            )
        );
        assertThat(e.getMessage(), containsString("field [b]"));
    }

    public void testAnEmptyMappingIsFine() throws Exception {
        validate("{}");
        ParquetMappingValidator.validate(FORMAT, List.of(), false);
    }

    public void testAMultiValuedDateIsRefusedLikeABoolean() throws Exception {
        assertRejected(
            "{\"properties\": {\"when\": {\"type\": \"date\", \"multi_value\": true}}}",
            "date field with several values",
            "[when]"
        );
        validate("{\"properties\": {\"when\": {\"type\": \"date\"}}}");
        for (String type : List.of("long", "integer", "short", "byte", "double", "float", "keyword")) {
            validate("{\"properties\": {\"f\": {\"type\": \"" + type + "\", \"multi_value\": true}}}");
        }
    }

    public void testAMultiValuedBooleanIsRefused() throws Exception {
        assertRejected(
            "{\"properties\": {\"flags\": {\"type\": \"boolean\", \"multi_value\": true}}}",
            "boolean field with several values",
            "[flags]"
        );
        assertRejected(
            "{\"properties\": {\"flags\": {\"type\": \"boolean\", \"doc_values\": {\"multi_value\": true}}}}",
            "boolean field with several values"
        );
        // the index default counts too, unless the field says otherwise
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetMappingValidator.validate(
                FORMAT,
                List.of(new CompressedXContent("{\"properties\": {\"flag\": {\"type\": \"boolean\"}}}")),
                true
            )
        );
        assertThat(e.getMessage(), containsString("[flag]"));
        ParquetMappingValidator.validate(
            FORMAT,
            List.of(new CompressedXContent("{\"properties\": {\"flag\": {\"type\": \"boolean\", \"multi_value\": false}}}")),
            true
        );
        validate("{\"properties\": {\"flag\": {\"type\": \"boolean\"}, \"tags\": {\"type\": \"keyword\", \"multi_value\": true}}}");
    }
}
