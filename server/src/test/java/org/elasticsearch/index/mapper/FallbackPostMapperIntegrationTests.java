/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.index.mapper.FieldStorageVerifier.forField;

public class FallbackPostMapperIntegrationTests extends MapperServiceTestCase {

    /**
     * Like {@link #syntheticSource} but skips the round-trip byte-equality check. Needed for
     * object-array inputs where per-element pre-capture tokens differ in raw bytes from the single
     * flat-array token produced when re-indexing the synthetic output, even though rendered text is identical.
     */
    private String syntheticSourceSkipRoundTrip(DocumentMapper mapper, CheckedConsumer<XContentBuilder, IOException> build)
        throws IOException {
        try (Directory directory = newDirectory()) {
            var iw = indexWriterForSyntheticSource(directory);
            ParsedDocument doc = mapper.parse(source(build));
            doc.updateSeqID(0, 0);
            doc.version().setLongValue(0);
            iw.addDocuments(doc.docs());
            iw.close();
            try (DirectoryReader reader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                return syntheticSource(mapper, reader, doc.docs().size() - 1);
            }
        }
    }

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
     * A {@link FieldMapper.SyntheticSourceMode#FALLBACK} field (e.g. numeric with {@code doc_values: false})
     * with {@code ignore_malformed: true} must commit its pre-capture even when parse returns {@code Ignored}.
     */
    public void testSyntheticFallbackWithIgnoreMalformedPreservesValueInSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "integer").field("doc_values", false).field("ignore_malformed", true))
        ).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("field", "not-a-number"));

        assertEquals("{\"field\":\"not-a-number\"}", syntheticSource);
    }

    /**
     * A field with {@code synthetic_source_keep: arrays} and {@code ignore_malformed: true} in an object
     * array must commit every per-element pre-capture: a partial {@code _ignored_source} array suppresses
     * the native loader entirely, silently dropping values served by {@code ._ignore_malformed}.
     */
    public void testSourceKeepArraysWithMixedMalformedInObjectArrayPreservesAllValues() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("obj");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("d");
                    {
                        b.field("type", "double");
                        b.field("synthetic_source_keep", "arrays");
                        b.field("ignore_malformed", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        // Object-array input produces per-element pre-captures whose raw bytes differ from the flat-array
        // token on re-index; use syntheticSourceSkipRoundTrip to avoid the byte-equality check.
        assertEquals("{\"obj\":{\"d\":[\"bad\",0.5]}}", syntheticSourceSkipRoundTrip(mapper, b -> {
            b.startArray("obj");
            b.startObject().field("d", "bad").endObject();
            b.startObject().field("d", 0.5).endObject();
            b.endArray();
        }));
    }

    /**
     * Same as {@link #testSourceKeepArraysWithMixedMalformedInObjectArrayPreservesAllValues} but with
     * {@code synthetic_source_keep: all}, exercising the {@link FallbackPostMapper.Reason#SOURCE_KEEP_ALL} path.
     */
    public void testSourceKeepAllWithMixedMalformedInObjectArrayPreservesAllValues() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("obj");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("d");
                    {
                        b.field("type", "double");
                        b.field("synthetic_source_keep", "all");
                        b.field("ignore_malformed", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        assertEquals("{\"obj\":{\"d\":[\"bad\",0.5]}}", syntheticSourceSkipRoundTrip(mapper, b -> {
            b.startArray("obj");
            b.startObject().field("d", "bad").endObject();
            b.startObject().field("d", 0.5).endObject();
            b.endArray();
        }));
    }

    /**
     * Flat field array (not object array): the entire array is pre-captured as one token, so a
     * {@code ParseResult.Ignored} result from the first element must still commit the pre-capture.
     */
    public void testSourceKeepAllWithMixedMalformedFlatArrayPreservesAllValues() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "double").field("synthetic_source_keep", "all").field("ignore_malformed", true))
        ).documentMapper();

        assertEquals("{\"field\":[\"bad\",0.5]}", syntheticSource(mapper, b -> {
            b.startArray("field");
            b.value("bad");
            b.value(0.5);
            b.endArray();
        }));
    }

    /**
     * Regression: a {@code keyword} field with {@code copy_to} pointing at a destination with
     * {@code synthetic_source_keep: all} must not duplicate the copied value in synthetic {@code _source}.
     * The copy-to traversal must inherit the recorded sub-context from the source field's pre-capture so
     * that {@code canAddIgnoredField()} is false during the traversal and no second
     * {@code _ignored_source} entry is written for the destination.
     */
    public void testCopyToDestinationWithSourceKeepAllDoesNotDuplicateCopiedValue() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("dest");
            {
                b.field("type", "keyword");
                b.field("synthetic_source_keep", "all");
            }
            b.endObject();
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.field("copy_to", "dest");
            }
            b.endObject();
        })).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> {
            b.field("dest", "own");
            b.field("src", "copied");
        });

        assertEquals("{\"dest\":\"own\",\"src\":\"copied\"}", syntheticSource);
    }

    /**
     * Regression: when the destination field has no own value, copied values must be absent from synthetic
     * {@code _source} entirely. This is the strongest assertion — it fails regardless of whether the void
     * placeholder is correctly installed — and specifically targets the missing context propagation that
     * allowed the copy-to traversal to write to {@code _ignored_source} when the destination has
     * {@code synthetic_source_keep: all}.
     */
    public void testCopyToDestinationWithSourceKeepAllAndNoOwnValueIsAbsentFromSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("dest");
            {
                b.field("type", "keyword");
                b.field("synthetic_source_keep", "all");
            }
            b.endObject();
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.field("copy_to", "dest");
            }
            b.endObject();
        })).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("src", "copied"));

        assertEquals("{\"src\":\"copied\"}", syntheticSource);
    }

    /**
     * Regression: the copy-to invariant holds for the {@link FallbackPostMapper.Reason#SYNTHETIC_FALLBACK}
     * path too, not only {@code SOURCE_KEEP_ALL}. A {@code keyword} field with {@code copy_to} pointing at
     * a fallback field (e.g. {@code integer} with {@code doc_values: false}) must not produce the copied
     * value in synthetic source.
     */
    public void testCopyToDestinationWithFallbackSyntheticSourceIsAbsentFromSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("doc_values", false);
            }
            b.endObject();
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.field("copy_to", "dest");
            }
            b.endObject();
        })).documentMapper();

        String syntheticSource = syntheticSource(mapper, b -> b.field("src", "5"));

        assertEquals("{\"src\":\"5\"}", syntheticSource);
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
