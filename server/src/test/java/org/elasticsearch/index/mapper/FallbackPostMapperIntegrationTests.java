/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for {@link FallbackPostMapper} covering the full parse → fallback-routing →
 * synthetic-source-reconstruction pipeline across mapper types and all pre-capture
 * {@link FallbackPostMapper.Reason}s.
 *
 * <p>"Integration" here means the complete parsing pipeline — XContent ingestion, Lucene document
 * construction, and synthetic-source reconstruction — not a cluster-level test. Certain observable
 * columns, particularly {@code ._on_failure}, cannot be inspected via the cluster API because
 * {@link OnFailureStoredValues} is currently write-only. Tests in this class assert directly on
 * the {@link ParsedDocument}'s {@link org.apache.lucene.document.Document} fields where cluster-
 * level assertions cannot reach.
 *
 * <p>Two production bugs are currently exposed as FAILS:
 * <ul>
 *   <li><b>Bug A</b>: {@link FallbackPostMapper#postParse} discards the pre-capture on
 *       {@link FieldMapper.ParseResult.Ignored} for non-{@link FieldMapper.SyntheticSourceMode#FALLBACK}
 *       mappers, including {@link FallbackPostMapper.Reason#COPY_TO_DESTINATION}. This silently
 *       drops a directly-supplied value at a {@code copy_to} destination when the copy-from source
 *       field is also present (the {@code voidValue()} entry written by {@code createCopyToContext}
 *       suppresses the {@code ._ignore_malformed} synthetic-source layer, leaving no other recovery
 *       path).</li>
 *   <li><b>Bug B</b>: {@link FieldMapper.MultiFields#parse} ignores the {@link FieldMapper.ParseResult}
 *       returned by each sub-field mapper, so {@link FieldMapper.ParseResult.MultiValueViolation}
 *       bytes from {@code geo_point} and {@code completion} multi-fields are never routed to
 *       {@code ._on_failure}. The fix is to apply the same handling already present in
 *       {@link FieldMapper#doParseMultiFields}.</li>
 * </ul>
 *
 * <p>Bug C ({@link ShardBatchMapper#parseMappings} bypasses {@link FallbackPostMapper#parseField},
 * losing pre-capture for {@link FieldMapper.SyntheticSourceMode#FALLBACK} fields) is tested in
 * {@code BatchBulkIT} because it requires a real {@code IndexShard}.
 */
public class FallbackPostMapperIntegrationTests extends MapperServiceTestCase {

    /**
     * Provide the standard analyzer required by {@link CompletionFieldMapper}, which looks up
     * the "default" analyzer by name during builder construction.
     */
    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())));
    }

    // -------------------------------------------------------------------------
    // Group 1 — MVV in a multi-field must reach ._on_failure
    //
    // Two production mappers call MultiFields.parse() directly instead of routing through
    // FieldMapper.doParseMultiFields:
    // GeoPointFieldMapper.java:321 (once per geometry element)
    // CompletionFieldMapper.java:449,457 (once per distinct input string)
    //
    // MultiFields.parse() discards the ParseResult, so MultiValueViolation bytes are lost.
    // doParseMultiFields (FieldMapper.java:347) correctly routes them; KeywordFieldMapper
    // exercises that path and serves as the control.
    // -------------------------------------------------------------------------

    /**
     * Bug B: {@link GeoPointFieldMapper} calls {@code multiFields().parse()} (the inner
     * {@link FieldMapper.MultiFields#parse} method) once per geometry element, discarding the
     * returned {@link FieldMapper.ParseResult}. With a two-element array the keyword sub-field is
     * parsed twice; the second parse returns {@link FieldMapper.ParseResult.MultiValueViolation},
     * but it is never written to {@code ._on_failure}.
     *
     * <p>Note: {@code _ignored} is still populated on the first parse because
     * {@code DocumentParserContext.enforceSingleValue} calls {@code addIgnoredField} independently
     * of the {@code ._on_failure} write. The old test that asserted only on {@code _ignored} was
     * therefore always green and non-discriminating.
     *
     * <p>This test FAILS currently (bug B).
     */
    public void testGeoPointMultiFieldMvvViolationReachesOnFailureColumn() throws IOException {
        assumeTrue("doc_values on_failure feature flag must be enabled", FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled());

        // The doc_values map form (multi_value / on_failure) is only accepted in columnar index
        // modes (FieldMapper.java:1761).
        DocumentMapper mapper = createColumnarModeDocumentMapper(mapping(b -> {
            b.startObject("location");
            {
                b.field("type", "geo_point");
                b.startObject("fields");
                {
                    b.startObject("kw");
                    {
                        b.field("type", "keyword");
                        b.startObject("doc_values").field("multi_value", false).field("on_failure", "ignore").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // GeoPointFieldMapper.index() is called once per geometry.
        // Each call invokes multiFields().parse() with a GeoHashMultiFieldParser, so "location.kw"
        // is parsed twice — MVV fires on the second parse.
        ParsedDocument doc = mapper.parse(
            source(
                b -> b.startArray("location")
                    .startObject()
                    .field("lat", 1.0)
                    .field("lon", 2.0)
                    .endObject()
                    .startObject()
                    .field("lat", 3.0)
                    .field("lon", 4.0)
                    .endObject()
                    .endArray()
            )
        );

        assertOnFailureColumnNotEmpty(
            "location.kw",
            doc,
            "MVV from the second geo_point element must reach location.kw._on_failure (bug B: "
                + "GeoPointFieldMapper.index() calls MultiFields.parse() which discards ParseResult)"
        );
        assertIgnoredField("location.kw", doc, "location.kw must be added to _ignored when the multi_value=false constraint is violated");
    }

    /**
     * Bug B: {@link CompletionFieldMapper} calls {@code multiFields().parse()} (the inner
     * {@link FieldMapper.MultiFields#parse} method) once per distinct input string, discarding the
     * returned {@link FieldMapper.ParseResult}. With two suggestions the keyword sub-field is parsed
     * twice; the second {@link FieldMapper.ParseResult.MultiValueViolation} is never written to
     * {@code ._on_failure}.
     *
     * <p>This test FAILS currently (bug B).
     */
    public void testCompletionMultiFieldMvvViolationReachesOnFailureColumn() throws IOException {
        assumeTrue("doc_values on_failure feature flag must be enabled", FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled());

        DocumentMapper mapper = createColumnarModeDocumentMapper(mapping(b -> {
            b.startObject("suggest");
            {
                b.field("type", "completion");
                b.startObject("fields");
                {
                    b.startObject("kw");
                    {
                        b.field("type", "keyword");
                        b.startObject("doc_values").field("multi_value", false).field("on_failure", "ignore").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // CompletionFieldMapper.parse() stores inputs in a map keyed by string. Two distinct strings
        // produce two entries, so multiFields().parse() is called twice → MVV on the second parse.
        ParsedDocument doc = mapper.parse(source(b -> b.array("suggest", "hello", "world")));

        assertOnFailureColumnNotEmpty(
            "suggest.kw",
            doc,
            "MVV from the second completion suggestion must reach suggest.kw._on_failure (bug B: "
                + "CompletionFieldMapper.parse() calls MultiFields.parse() which discards ParseResult)"
        );
        assertIgnoredField("suggest.kw", doc, "suggest.kw must be added to _ignored when the multi_value=false constraint is violated");
    }

    /**
     * Control for bug B: {@link KeywordFieldMapper} goes through the standard {@link FieldMapper#parse}
     * path which calls {@link FieldMapper#doParseMultiFields}. That method correctly handles
     * {@link FieldMapper.ParseResult.MultiValueViolation} by invoking
     * {@link OnFailureStoredValues#storeEncoded}, so the MVV must reach {@code ._on_failure}.
     *
     * <p>This test PASSES today (control — shows {@link FieldMapper#doParseMultiFields} is correct).
     */
    public void testKeywordMultiFieldMvvViolationReachesOnFailureColumnViaDoParseMultiFields() throws IOException {
        assumeTrue("doc_values on_failure feature flag must be enabled", FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled());

        // Parent keyword has no MVV constraint; sub-field kw has multi_value=false.
        DocumentMapper mapper = createColumnarModeDocumentMapper(mapping(b -> {
            b.startObject("parent");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                {
                    b.startObject("kw");
                    {
                        b.field("type", "keyword");
                        b.startObject("doc_values").field("multi_value", false).field("on_failure", "ignore").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Two distinct values → the parent keyword is parsed twice → doParseMultiFields calls
        // kw.parse() twice → MVV on the second → doParseMultiFields routes the result correctly.
        ParsedDocument doc = mapper.parse(source(b -> b.array("parent", "a", "b")));

        assertOnFailureColumnNotEmpty(
            "parent.kw",
            doc,
            "MVV from the second keyword value must reach parent.kw._on_failure via doParseMultiFields"
        );
        assertIgnoredField("parent.kw", doc, "parent.kw must be added to _ignored when the multi_value=false constraint is violated");
    }

    // -------------------------------------------------------------------------
    // Group 2 — copy_to destination + ignore_malformed
    //
    // When a copy_to source is present, DocumentParserContext.createCopyToContext()
    // (DocumentParserContext.java:1174-1193) writes a voidValue() _ignored_source entry for
    // the destination, which suppresses the copy-from invocation's _ignored_source entry.
    // A directly-supplied malformed value at the destination can then be recovered ONLY by
    // a committed COPY_TO_DESTINATION pre-capture.
    //
    // Bug A: postParse discards the COPY_TO_DESTINATION pre-capture on ParseResult.Ignored
    // for non-FALLBACK mappers (FallbackPostMapper.java:207), so the direct malformed value
    // at the destination is silently lost when the copy-from source is also present.
    // -------------------------------------------------------------------------

    /**
     * Bug A: with both a copy-from source ({@code src}) and a directly malformed value at the
     * destination ({@code dest}) in the same document, the destination's pre-capture is discarded
     * on {@link FieldMapper.ParseResult.Ignored} because the mapper is
     * {@link FieldMapper.SyntheticSourceMode#NATIVE}. The {@code voidValue()} entry written by
     * {@code createCopyToContext} then suppresses the {@code ._ignore_malformed} synthetic-source
     * layer, leaving the direct malformed value irrecoverable.
     *
     * <p>This test FAILS currently (bug A).
     */
    public void testCopyToDestinationMalformedValueNotDroppedWhenCopyToSourcePresent() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.array("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
            }
            b.endObject();
        })).documentMapper();

        // dest="not-a-number" is supplied directly in the document.
        // When src is also present, createCopyToContext() writes a voidValue() _ignored_source
        // entry for dest — the only path to preserve the direct malformed value is a committed
        // COPY_TO_DESTINATION pre-capture.
        String source = syntheticSource(mapper, b -> b.field("src", "123").field("dest", "not-a-number"));

        // Compare as maps to avoid brittle key-order assertions.
        Map<String, Object> parsed = parseJson(source);
        assertThat("src must be in synthetic source", parsed.get("src"), equalTo("123"));
        assertThat(
            "malformed dest value must not be dropped from synthetic source when src also provides copy_to (bug A: "
                + "postParse discards the COPY_TO_DESTINATION pre-capture on Ignored for non-FALLBACK mappers)",
            parsed.get("dest"),
            equalTo("not-a-number")
        );
    }

    /**
     * Control for bug A: without a copy-from source, the {@code voidValue()} {@code _ignored_source}
     * entry is never written for {@code dest}, so the {@code ._ignore_malformed} synthetic-source
     * layer restores the malformed value normally.
     *
     * <p>This test PASSES today (validates the mapping setup used by the failing test above).
     */
    public void testCopyToDestinationMalformedValueRestoredWithoutCopyToSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.array("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
            }
            b.endObject();
        })).documentMapper();

        // Without src, createCopyToContext() is never called for dest → no voidValue() entry →
        // ._ignore_malformed layer recovers the value normally.
        String source = syntheticSource(mapper, b -> b.field("dest", "not-a-number"));

        Map<String, Object> parsed = parseJson(source);
        assertThat(
            "malformed dest value must appear in synthetic source via ._ignore_malformed when src is absent",
            parsed.get("dest"),
            equalTo("not-a-number")
        );
    }

    // -------------------------------------------------------------------------
    // Group 3 — Indexed / Ignored branches of postParse across pre-capture reasons
    //
    // resolvePrecaptureReason returns one of: SYNTHETIC_FALLBACK, SOURCE_KEEP_ALL,
    // SOURCE_KEEP_ARRAYS_IN_ARRAY, or COPY_TO_DESTINATION.
    //
    // postParse handles ParseResult.Ignored as follows:
    // - commit the pre-capture if syntheticSourceMode == FALLBACK
    // - discard the pre-capture otherwise (non-FALLBACK mappers rely on ._ignore_malformed)
    //
    // Bug A lives in the COPY_TO_DESTINATION × Ignored cell when the copy-from source is present.
    // -------------------------------------------------------------------------

    // -- SYNTHETIC_FALLBACK --

    /**
     * {@link FieldMapper.SyntheticSourceMode#FALLBACK} + {@link FieldMapper.ParseResult.Indexed}:
     * pre-capture must be committed → value appears in {@code _ignored_source} and in synthetic source.
     *
     * <p>This test PASSES today.
     */
    public void testSyntheticFallbackIndexedCommitsPrecaptureToIgnoredSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "keyword");
                b.field("doc_values", false);
                b.field("store", false);
            }
            b.endObject();
        })).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "hello")));

        assertIgnoredSourceNotEmpty(doc, "FALLBACK field with a successful parse must be pre-captured in _ignored_source on Indexed");
        assertThat(
            "FALLBACK field must appear in synthetic source via _ignored_source",
            syntheticSource(mapper, b -> b.field("field", "hello")),
            equalTo("{\"field\":\"hello\"}")
        );
    }

    /**
     * {@link FieldMapper.SyntheticSourceMode#FALLBACK} + {@link FieldMapper.ParseResult.Ignored}:
     * pre-capture must still be committed (FALLBACK mode commits on Ignored, not just on Indexed) →
     * value appears in {@code _ignored_source} and in synthetic source.
     *
     * <p>Triggered via {@code ignore_above} so the mapper returns {@code Ignored}.
     *
     * <p>This test PASSES today (was broken before because the class-wide
     * {@code indices.batch_indexing=true} routed all parses through {@link ShardBatchMapper},
     * which bypasses pre-capture entirely — bug C).
     */
    public void testSyntheticFallbackIgnoredCommitsPrecaptureToIgnoredSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "keyword");
                b.field("doc_values", false);
                b.field("store", false);
                b.field("ignore_above", 5);
            }
            b.endObject();
        })).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "hello world")));

        assertIgnoredSourceNotEmpty(
            doc,
            "FALLBACK field must still commit pre-capture to _ignored_source when Ignored (ignore_above exceeded)"
        );
        assertThat(
            "FALLBACK field value exceeding ignore_above must appear in synthetic source",
            syntheticSource(mapper, b -> b.field("field", "hello world")),
            equalTo("{\"field\":\"hello world\"}")
        );
    }

    // -- SOURCE_KEEP_ALL --

    /**
     * {@link FallbackPostMapper.Reason#SOURCE_KEEP_ALL} + {@link FieldMapper.ParseResult.Indexed}:
     * pre-capture must be committed → value appears in {@code _ignored_source}.
     *
     * <p>This test PASSES today.
     */
    public void testSourceKeepAllIndexedCommitsPrecaptureToIgnoredSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "integer");
                b.field("synthetic_source_keep", "all");
            }
            b.endObject();
        })).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 42)));

        assertIgnoredSourceNotEmpty(doc, "SOURCE_KEEP_ALL field with a valid value must be pre-captured in _ignored_source on Indexed");
    }

    /**
     * {@link FallbackPostMapper.Reason#SOURCE_KEEP_ALL} + {@link FieldMapper.ParseResult.Ignored}:
     * pre-capture is discarded for non-FALLBACK mappers. This is the intended behaviour — the mapper
     * writes the value to {@code ._ignore_malformed} directly, and the
     * {@link CompositeSyntheticFieldLoader} malformed-values layer recovers it. No entry in
     * {@code _ignored_source} is expected.
     *
     * <p>This test PASSES today. (Existing focused coverage:
     * {@code DocValuesParameterTests#testNonFallbackMalformedDiscardsPreCaptureFromIgnoredSource}.)
     */
    public void testSourceKeepAllIgnoredDiscardsNonFallbackPrecapture() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
                b.field("synthetic_source_keep", "all");
            }
            b.endObject();
        })).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "not-a-number")));

        assertIgnoredSourceEmpty(
            doc,
            "pre-capture must be discarded for non-FALLBACK mapper on Ignored: value must not appear in _ignored_source"
        );
        assertThat(
            "malformed integer must appear in synthetic source via ._ignore_malformed (not via _ignored_source)",
            syntheticSource(mapper, b -> b.field("field", "not-a-number")),
            equalTo("{\"field\":\"not-a-number\"}")
        );
    }

    // -- COPY_TO_DESTINATION --

    /**
     * {@link FallbackPostMapper.Reason#COPY_TO_DESTINATION} + {@link FieldMapper.ParseResult.Indexed}:
     * pre-capture must be committed → value appears in {@code _ignored_source}.
     *
     * <p>This test PASSES today.
     */
    public void testCopyToDestinationIndexedCommitsPrecaptureToIgnoredSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.array("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
            }
            b.endObject();
        })).documentMapper();

        // dest is supplied directly (no src → no copy_to traversal).
        // isCopyToDestinationField(dest) == true → COPY_TO_DESTINATION pre-capture → committed on Indexed.
        ParsedDocument doc = mapper.parse(source(b -> b.field("dest", 42)));

        assertIgnoredSourceNotEmpty(
            doc,
            "COPY_TO_DESTINATION field with a valid direct value must have its pre-capture committed to _ignored_source"
        );
    }

    /**
     * {@link FallbackPostMapper.Reason#COPY_TO_DESTINATION} + {@link FieldMapper.ParseResult.Ignored}
     * without a copy-from source: pre-capture is discarded (mapper is NATIVE), but the
     * {@code ._ignore_malformed} synthetic-source layer still restores the value because no
     * {@code voidValue()} entry was written for the destination.
     *
     * <p>This test PASSES today. Contrast with
     * {@link #testCopyToDestinationMalformedValueNotDroppedWhenCopyToSourcePresent} where the
     * copy-from source suppresses that recovery path.
     */
    public void testCopyToDestinationIgnoredWithoutSrcRestoredViaIgnoreMalformed() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("src");
            {
                b.field("type", "keyword");
                b.array("copy_to", "dest");
            }
            b.endObject();
            b.startObject("dest");
            {
                b.field("type", "integer");
                b.field("ignore_malformed", true);
            }
            b.endObject();
        })).documentMapper();

        // Without src, no voidValue() entry → ._ignore_malformed layer works normally.
        String source = syntheticSource(mapper, b -> b.field("dest", "not-a-number"));

        Map<String, Object> parsed = parseJson(source);
        assertThat(
            "malformed dest value must appear in synthetic source via ._ignore_malformed when src is absent",
            parsed.get("dest"),
            equalTo("not-a-number")
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Asserts that the {@code <field>._on_failure} binary column in the root Lucene document is
     * non-empty, verifying that a {@link FieldMapper.ParseResult.MultiValueViolation} was written
     * via {@link OnFailureStoredValues#storeEncoded}.
     */
    private void assertOnFailureColumnNotEmpty(String field, ParsedDocument doc, String message) {
        List<IndexableField> column = doc.rootDoc().getFields(field + OnFailureStoredValues.ON_FAILURE_FIELD_NAME_SUFFIX);
        assertThat(message, column.isEmpty(), equalTo(false));
    }

    /**
     * Asserts that {@code field} appears in the {@code _ignored} stored field of the root document.
     */
    private void assertIgnoredField(String field, ParsedDocument doc, String message) {
        assertTrue(message, doc.rootDoc().getFields("_ignored").stream().anyMatch(f -> field.equals(f.stringValue())));
    }

    /**
     * Asserts that at least one {@code _ignored_source} blob was written to the root document,
     * indicating that at least one pre-capture was committed.
     */
    private void assertIgnoredSourceNotEmpty(ParsedDocument doc, String message) {
        assertNotNull(message, doc.rootDoc().getField(IgnoredSourceFieldMapper.NAME));
    }

    /**
     * Asserts that no {@code _ignored_source} blob was written to the root document.
     */
    private void assertIgnoredSourceEmpty(ParsedDocument doc, String message) {
        assertTrue(message, doc.rootDoc().getFields(IgnoredSourceFieldMapper.NAME).isEmpty());
    }

    /**
     * Parses a JSON string and returns the root object as a map.
     */
    private Map<String, Object> parseJson(String json) throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), json)) {
            return parser.map();
        }
    }
}
