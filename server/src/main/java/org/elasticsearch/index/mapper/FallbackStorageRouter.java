/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Central router for fallback storage decisions.
 * <p>
 * When a document field value cannot be indexed normally — because it is malformed, violates a
 * cardinality constraint, or cannot be reconstructed from doc values in synthetic source mode —
 * it must be diverted to one of several alternative storage locations so the value is not silently
 * dropped. This class is the single place that knows which destination to use for which failure.
 *
 * <h2>Destinations</h2>
 * <ul>
 *   <li>{@link FallbackStorageDestination#IGNORED_SOURCE} — {@code _ignored_source} metadata field,
 *       used for synthetic source reconstruction of fields in fallback mode, fields with
 *       {@code source_keep}, copy-to destinations, and unmapped fields under dynamic: false/runtime.</li>
 *   <li>{@link FallbackStorageDestination#IGNORE_MALFORMED} — per-field {@code ._ignore_malformed}
 *       column, used when a value fails to parse with {@code ignore_malformed: true}.</li>
 *   <li>{@link FallbackStorageDestination#ON_FAILURE} — per-field {@code ._on_failure} column,
 *       used when a {@code multi_value: false} cardinality constraint is violated with
 *       {@code on_failure: ignore}.</li>
 * </ul>
 *
 * <h2>Usage patterns</h2>
 * <h3>Immediate write (encode and commit in one call)</h3>
 * <pre>{@code
 * FallbackStorageRouter.write(context, fieldPath, Reason.MALFORMED, parser);
 * }</pre>
 *
 * <h3>Deferred write (capture now, commit later)</h3>
 * <pre>{@code
 * FallbackStorageRouter.Request pending = FallbackStorageRouter.capture(fieldPath, Reason.MULTI_VALUE_VIOLATION, parser);
 * // ... do other work ...
 * FallbackStorageRouter.commit(context, pending);
 * }</pre>
 *
 * <h3>Pre-capture gate for {@code _ignored_source} (used in document parsing)</h3>
 * <pre>{@code
 * if (FallbackStorageRouter.shouldPreCaptureToIgnoredSource(context, fieldMapper, sourceKeepMode, parsesArrayValue)) {
 *     context = context.addIgnoredFieldFromContext(NameValue.fromContext(context, fieldMapper.fullPath(), null));
 * }
 * fieldMapper.parse(context);
 * }</pre>
 */
public final class FallbackStorageRouter {

    private FallbackStorageRouter() {}

    // -------------------------------------------------------------------------
    // Reason enum — WHY a value is being redirected to fallback storage
    // -------------------------------------------------------------------------

    /**
     * The reason a field value is being redirected to fallback storage.
     * <p>
     * The reason alone determines the {@link FallbackStorageDestination}; see {@link #route}.
     */
    public enum Reason {
        /**
         * The value failed to parse with {@code ignore_malformed: true}.
         * Routes to {@link FallbackStorageDestination#IGNORE_MALFORMED}.
         */
        MALFORMED,

        /**
         * A {@code multi_value: false} field received a second value in the same document and the
         * field is configured with {@code on_failure: ignore}.
         * Routes to {@link FallbackStorageDestination#ON_FAILURE}.
         */
        MULTI_VALUE_VIOLATION,

        /**
         * The field uses synthetic source fallback mode ({@link FieldMapper.SyntheticSourceMode#FALLBACK})
         * and cannot reconstruct its value purely from doc values.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        SYNTHETIC_FALLBACK,

        /**
         * The field or object is configured with {@code source_keep: all}.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        SOURCE_KEEP_ALL,

        /**
         * The field is inside an array scope on a parent with {@code source_keep: arrays}, and the
         * mapper does not natively parse arrays.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        SOURCE_KEEP_ARRAYS_IN_ARRAY,

        /**
         * The field is a {@code copy_to} destination (and the parse is not itself a copy-to traversal).
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        COPY_TO_DESTINATION,

        /**
         * The field is unmapped under a parent with {@code dynamic: false}.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        DYNAMIC_DISABLED,

        /**
         * The field is unmapped under a parent with {@code dynamic: runtime}.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        DYNAMIC_RUNTIME,

        /**
         * The field is inside a disabled object mapper ({@code enabled: false}).
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        OBJECT_DISABLED,

        /**
         * A dynamic field was not indexed because the index's total field count would exceed
         * {@code index.mapping.total_fields.limit}.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        FIELD_LIMIT_EXCEEDED,

        /**
         * A dynamic field was not indexed because the field's name exceeds
         * {@code index.mapping.field_name_length.limit}.
         * Routes to {@link FallbackStorageDestination#IGNORED_SOURCE}.
         */
        FIELD_NAME_TOO_LONG;
    }

    // -------------------------------------------------------------------------
    // Request record — context for the routing decision and deferred-write container
    // -------------------------------------------------------------------------

    /**
     * Captures the context needed to route a fallback write: the target field path, the reason the
     * value is being redirected, and the value itself pre-encoded as a {@link BytesRef}.
     * <p>
     * Instances are cheap to create at any point during document parsing (even before the
     * {@link DocumentParserContext} is available) and can be committed later via
     * {@link FallbackStorageRouter#commit}. This makes deferred routing straightforward: encode the
     * value as soon as it is available, decide the destination, and commit once the context is ready.
     */
    public record Request(String fieldPath, Reason reason, BytesRef encodedValue) {}

    // -------------------------------------------------------------------------
    // Routing decision (pure, no I/O)
    // -------------------------------------------------------------------------

    /**
     * Maps a {@link Reason} to its {@link FallbackStorageDestination}.
     * This is the single place in the codebase that defines the reason → destination mapping.
     */
    public static FallbackStorageDestination route(Reason reason) {
        return switch (reason) {
            case MALFORMED -> FallbackStorageDestination.IGNORE_MALFORMED;
            case MULTI_VALUE_VIOLATION -> FallbackStorageDestination.ON_FAILURE;
            case SYNTHETIC_FALLBACK, SOURCE_KEEP_ALL, SOURCE_KEEP_ARRAYS_IN_ARRAY, COPY_TO_DESTINATION, DYNAMIC_DISABLED, DYNAMIC_RUNTIME,
                OBJECT_DISABLED, FIELD_LIMIT_EXCEEDED, FIELD_NAME_TOO_LONG -> FallbackStorageDestination.IGNORED_SOURCE;
        };
    }

    // -------------------------------------------------------------------------
    // Pre-capture gate (for _ignored_source pre-capture in DocumentParser)
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if a field mapper should have its entire XContent pre-captured to
     * {@code _ignored_source} before {@link FieldMapper#parse} is called.
     * <p>
     * Pre-capturing is needed when the mapper cannot reconstruct the original value from its own
     * doc values during synthetic source reconstruction, or when the field is explicitly configured
     * to preserve its source. Pre-capture works by cloning the live {@link XContentParser} at the
     * current position (via {@link DocumentParserContext#addIgnoredFieldFromContext}) before parsing
     * begins, so the value is retained verbatim regardless of what the mapper actually indexes.
     * <p>
     * Pre-capture is intentionally skipped for fields that redirect {@code multi_value: false}
     * cardinality violations to {@code ._on_failure}: a duplicate value must land in exactly one
     * storage location, and the {@code on_failure: ignore} path already handles that write.
     *
     * @param context               the current document parsing context
     * @param fieldMapper           the field mapper about to be parsed
     * @param sourceKeepMode        the effective {@link Mapper.SourceKeepMode} for this field
     * @param mapperParsesArrayValue whether the mapper natively parses array values
     *                              (i.e. {@code mapper instanceof FieldMapper fm && fm.parsesArrayValue()})
     * @return {@code true} if the field's content must be pre-captured to {@code _ignored_source}
     */
    public static boolean shouldPreCaptureToIgnoredSource(
        DocumentParserContext context,
        FieldMapper fieldMapper,
        Mapper.SourceKeepMode sourceKeepMode,
        boolean mapperParsesArrayValue
    ) {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        // A multi_value=false field configured with on_failure=ignore writes its extra values to
        // ._on_failure, not _ignored_source. Routing it through the pre-capture path would write the
        // first (accepted) occurrence to _ignored_source AND index it normally — double-storing it.
        if (fieldMapper.isSingleValueEnforced()
            && fieldMapper.onFailureBehavior() == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
            return false;
        }
        return fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK
            || sourceKeepMode == Mapper.SourceKeepMode.ALL
            || (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS && context.inArrayScope() && mapperParsesArrayValue == false)
            || (context.isWithinCopyTo() == false && context.isCopyToDestinationField(fieldMapper.fullPath()));
    }

    // -------------------------------------------------------------------------
    // Immediate write
    // -------------------------------------------------------------------------

    /**
     * Encodes the current parser token and immediately writes it to the appropriate fallback
     * storage destination for the given reason.
     * <p>
     * Equivalent to {@code commit(context, capture(fieldPath, reason, parser))} but avoids
     * allocating an intermediate {@link Request} when the context is already available.
     *
     * @param context   the current document parsing context; the encoded value is written to its
     *                  Lucene document
     * @param fieldPath the full path of the field whose fallback column the value is stored under
     * @param reason    why this value is being redirected to fallback storage
     * @param parser    positioned at the value to encode and store
     * @throws IOException if encoding or writing fails
     */
    public static void write(DocumentParserContext context, String fieldPath, Reason reason, XContentParser parser) throws IOException {
        switch (route(reason)) {
            case IGNORED_SOURCE -> context.addIgnoredField(
                IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, XContentDataHelper.encodeToken(parser))
            );
            case IGNORE_MALFORMED -> IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fieldPath, parser);
            case ON_FAILURE -> {
                // Stored source already retains the offending value verbatim; only synthetic source
                // (and columnar_stored, which reconstructs its per-document source the same way)
                // need the value captured in the failure column to survive reconstruction.
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeValueForOnFailureIgnore(context, fieldPath, parser);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Deferred (lazy) write
    // -------------------------------------------------------------------------

    /**
     * Encodes the current parser token into a {@link Request} without writing it to any Lucene
     * field yet. The write is committed later via {@link #commit}.
     * <p>
     * Use the deferred pattern when the {@link DocumentParserContext} is not yet available at
     * encoding time, or when multiple writes should be batched and committed together.
     *
     * @param fieldPath the full path of the field whose fallback column the value will be stored under
     * @param reason    why this value is being redirected to fallback storage
     * @param parser    positioned at the value to encode
     * @return a {@link Request} that can be committed later
     * @throws IOException if encoding fails
     */
    public static Request capture(String fieldPath, Reason reason, XContentParser parser) throws IOException {
        return new Request(fieldPath, reason, XContentDataHelper.encodeToken(parser));
    }

    /**
     * Commits a previously {@linkplain #capture captured} fallback write to the appropriate
     * storage destination.
     *
     * @param context the current document parsing context; the encoded value is written to its
     *                Lucene document
     * @param request the deferred write produced by {@link #capture}
     * @throws IOException if writing fails
     */
    public static void commit(DocumentParserContext context, Request request) throws IOException {
        switch (route(request.reason())) {
            case IGNORED_SOURCE -> context.addIgnoredField(
                IgnoredSourceFieldMapper.NameValue.fromContext(context, request.fieldPath(), request.encodedValue())
            );
            case IGNORE_MALFORMED -> IgnoreMalformedStoredValues.writeEncoded(context, request.fieldPath(), request.encodedValue());
            case ON_FAILURE -> {
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeEncoded(context, request.fieldPath(), request.encodedValue());
                }
            }
        }
    }
}
