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
     * Computes synthetic source without the round-trip stored-field equality check.
     *
     * <p>The standard {@link #syntheticSource(DocumentMapper, org.elasticsearch.core.CheckedConsumer)}
     * helper calls {@code validateRoundTripReader}, which asserts that re-indexing the synthetic
     * source string produces byte-identical stored fields. That assertion legitimately fails when the
     * original document uses an object-array structure that gets flattened in the synthetic source:
     * the per-element pre-capture tokens written on the first pass differ in raw bytes from the
     * single flat-array token captured on the round-trip, even though the rendered output is the same.
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
     * Regression: a double field with {@code synthetic_source_keep: arrays} and
     * {@code ignore_malformed: true} that receives values via an object array must preserve every
     * value — including the first malformed one — in {@code _ignored_source}.
     *
     * <p>Root cause of the regression: {@code FallbackPostMapper.postParse} discarded the
     * per-element pre-capture when {@link FieldMapper.ParseResult} was {@code Ignored}. Because
     * {@link FieldMapper#resolveIgnoredResult} is edge-triggered (at most one value per field per
     * document can return {@code Ignored}), later valid values would commit their own pre-captures,
     * leaving a partial {@code _ignored_source} array. On the read side, any {@code _ignored_source}
     * entry for a field suppresses that field's native loader entirely — including the
     * {@code ._ignore_malformed} reader — so the malformed value was silently lost.
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

        // Both values must survive in synthetic source: the malformed "bad" and the valid 0.5.
        // Before the fix, only 0.5 appeared (the "bad" pre-capture was discarded, leaving a partial
        // _ignored_source that then suppressed the ._ignore_malformed reader).
        //
        // Round-trip stored-field equality is intentionally not checked here: per-element pre-captures
        // produce different raw _ignored_source bytes than the single flat-array token captured when
        // the round-trip re-indexes {"obj":{"d":["bad",0.5]}}. The rendered output is identical.
        assertEquals("{\"obj\":{\"d\":[\"bad\",0.5]}}", syntheticSourceSkipRoundTrip(mapper, b -> {
            b.startArray("obj");
            b.startObject().field("d", "bad").endObject();
            b.startObject().field("d", 0.5).endObject();
            b.endArray();
        }));
    }

    /**
     * Variant of the regression test using {@code synthetic_source_keep: all}, which exercises the
     * {@link FallbackPostMapper.Reason#SOURCE_KEEP_ALL} branch rather than
     * {@link FallbackPostMapper.Reason#SOURCE_KEEP_ARRAYS_IN_ARRAY}.
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
     * Variant where the array is at the root (flat field array), not inside an object array.
     * A flat array is captured as a single XContent token — the whole array is pre-captured once.
     * The {@code ParseResult.Ignored} case (from the first element being malformed) must still
     * commit that pre-capture rather than discarding it.
     */
    public void testSourceKeepAllWithMixedMalformedFlatArrayPreservesAllValues() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "double").field("synthetic_source_keep", "all").field("ignore_malformed", true))
        ).documentMapper();

        // The entire ["bad", 0.5] array is pre-captured as one token. Before the fix the
        // ParseResult.Ignored result caused the pre-capture to be discarded, so only 0.5
        // appeared in the synthetic source (via doc values). With the fix the full array is
        // committed to _ignored_source and rendered intact.
        assertEquals("{\"field\":[\"bad\",0.5]}", syntheticSource(mapper, b -> {
            b.startArray("field");
            b.value("bad");
            b.value(0.5);
            b.endArray();
        }));
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
