/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

import static org.elasticsearch.index.mapper.FieldStorageVerifier.forField;

public class FallbackPostMapperIntegrationTests extends MapperServiceTestCase {

    public void testCopyToDestinationWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.field("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
            }
            b.endObject();
        })).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> {
            b.field("src", "123");
            b.field("dest", "not-a-number");
        });

        assertEquals("{\"dest\":\"not-a-number\",\"src\":\"123\"}", syntheticSource);
    }

    /**
     * Verifies that {@code source_keep: all + ignore_malformed} still preserves the malformed value in synthetic source.
     */
    public void testSourceKeepAllWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "integer").field("synthetic_source_keep", "all").field("ignore_malformed", true))
        ).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("field", "not-a-number"));

        assertEquals("{\"field\":\"not-a-number\"}", syntheticSource);
    }

    /**
     * Verifies that when a mapper using {@link FieldMapper.SyntheticSourceMode#FALLBACK} (e.g. a
     * numeric field with {@code doc_values: false}) also has {@code ignore_malformed: true} and
     * receives a malformed value, the {@code SYNTHETIC_FALLBACK} pre-capture is committed to
     * {@code _ignored_source} rather than discarded. This exercises the
     * {@code precaptureReason == SYNTHETIC_FALLBACK} branch in {@code FallbackPostMapper.postParse}.
     */
    public void testSyntheticFallbackWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "integer").field("doc_values", false).field("ignore_malformed", true))
        ).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("field", "not-a-number"));

        assertEquals("{\"field\":\"not-a-number\"}", syntheticSource);
    }

    /**
     * Regression test: a {@code geo_point} field with a {@code keyword} multi-field that has
     * {@code multi_value: false, on_failure: ignore} must store the violating (second) value in
     * {@code field.kw._on_failure} when the document supplies two geo_point values.
     */
    public void testGeoPointMultiFieldMultiValueViolationStoredInOnFailure() throws IOException {
        // multi_value: false is only supported in columnar mode
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "geo_point");
                b.startObject("fields");
                {
                    b.startObject("kw");
                    {
                        b.field("type", "keyword");
                        b.startObject("doc_values");
                        {
                            b.field("multi_value", false);
                            b.field("on_failure", "ignore");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }), true).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.array("field", "40,30", "50,40")));

        // first value is indexed normally into doc values; second value must land in ._on_failure
        forField("field.kw", doc.rootDoc()).expectDocValues().expectOnFailure().verify();
    }

}
