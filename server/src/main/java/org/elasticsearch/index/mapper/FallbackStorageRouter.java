/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Optional;

/**
 * Central router for fallback storage decisions.
 * <p>
 * When a document field value cannot be indexed normally — because it is malformed, violates a
 * cardinality constraint, or cannot be reconstructed from doc values in synthetic source mode —
 * it must be diverted to one of several alternative storage locations so the value is not silently
 * dropped. This class is the <em>single</em> place that knows which destination to use for which
 * failure, and it owns all writes to those destinations.
 *
 * <h2>Destinations</h2>
 * <ul>
 *   <li>{@link Destination#IGNORED_SOURCE} — {@code _ignored_source} metadata field,
 *       used for synthetic source reconstruction of fields in fallback mode, fields with
 *       {@code source_keep}, copy-to destinations, and unmapped fields under dynamic: false/runtime.</li>
 *   <li>{@link Destination#IGNORE_MALFORMED} — per-field {@code ._ignore_malformed}
 *       column, used when a value fails to parse with {@code ignore_malformed: true}.</li>
 *   <li>{@link Destination#ON_FAILURE} — per-field {@code ._on_failure} column,
 *       used when a {@code multi_value: false} cardinality constraint is violated with
 *       {@code on_failure: ignore}.</li>
 * </ul>
 *
 * <h2>Pre-capture routing</h2>
 * <p>
 * The central routing decision for field-level pre-capture — <em>should we capture a field's full
 * XContent to {@code _ignored_source} before parsing it?</em> — is made entirely within this class
 * via {@link #resolvePrecaptureReason}. Callers supply a {@link FieldContext} record containing all
 * state needed for the decision (pulled from the live {@link DocumentParserContext} and
 * {@link FieldMapper} before calling), so the decision logic has no external dependencies and is
 * straightforward to unit-test by constructing {@link FieldContext} values directly.
 *
 * <pre>{@code
 * // Regular field parse path:
 * var fc = FallbackStorageRouter.FieldContext.forField(context, fieldMapper, parsesArrayValue);
 * Optional<FallbackStorageRouter.Reason> preCapReason = FallbackStorageRouter.resolvePrecaptureReason(fc);
 * if (preCapReason.isPresent()) {
 *     context = FallbackStorageRouter.preCaptureToIgnoredSource(context, fieldMapper.fullPath(), preCapReason.get());
 * }
 * fieldMapper.parse(context);
 *
 * // Array elements path:
 * var fc = FallbackStorageRouter.FieldContext.forArrayElements(context, mapper, fullPath);
 * Optional<FallbackStorageRouter.Reason> preCapReason = FallbackStorageRouter.resolvePrecaptureReason(fc);
 * if (preCapReason.isPresent()) {
 *     context = FallbackStorageRouter.preCaptureToIgnoredSource(context, fullPath, preCapReason.get());
 * }
 * }</pre>
 *
 * <h2>Immediate writes</h2>
 * <pre>{@code
 * // Malformed / cardinality violations:
 * FallbackStorageRouter.write(context, fieldPath, Reason.MALFORMED, parser);
 *
 * // Immediate write to _ignored_source (field is a child of context.parent()):
 * FallbackStorageRouter.writeToIgnoredSource(context, fieldPath, Reason.DYNAMIC_DISABLED);
 *
 * // Immediate write to _ignored_source (context.parent() IS the field being stored):
 * FallbackStorageRouter.writeParentToIgnoredSource(context, Reason.OBJECT_DISABLED);
 *
 * // Pre-capture context.parent() to _ignored_source (object source_keep):
 * context = FallbackStorageRouter.preCaptureParentToIgnoredSource(context, Reason.SOURCE_KEEP_ALL);
 * }</pre>
 */
public final class FallbackStorageRouter {

    private FallbackStorageRouter() {}

    // -------------------------------------------------------------------------
    // Destination enum — WHERE a redirected value is stored
    // -------------------------------------------------------------------------

    /**
     * The possible storage destinations for a document field value that cannot be indexed normally.
     * <p>
     * Every fallback write lands in exactly one destination; the choice is made by {@link #route}.
     */
    public enum Destination {

        /**
         * The value is preserved in the {@code _ignored_source} metadata field and included verbatim
         * in synthetic {@code _source} reconstruction. Used for fields that cannot reconstruct their
         * value from doc values (synthetic source fallback mode), fields with {@code source_keep},
         * copy-to destinations, and unmapped fields under {@code dynamic: false} or
         * {@code dynamic: runtime}.
         */
        IGNORED_SOURCE,

        /**
         * The value is stored in a per-field {@code fieldPath._ignore_malformed} column (binary doc
         * values on new indices; stored field on old indices). Used when a value fails to parse with
         * {@code ignore_malformed: true}, so that synthetic {@code _source} reconstruction can still
         * reproduce the original value verbatim.
         */
        IGNORE_MALFORMED,

        /**
         * The value is stored in a per-field {@code fieldPath._on_failure} binary doc values column.
         * Used when a {@code multi_value: false} field receives a duplicate value and the field is
         * configured with {@code doc_values.on_failure: ignore}, so that indexing continues without
         * the excess value reaching the field's own doc values.
         */
        ON_FAILURE;
    }

    // -------------------------------------------------------------------------
    // Reason enum — WHY a value is being redirected to fallback storage
    // -------------------------------------------------------------------------

    /**
     * The reason a field value is being redirected to fallback storage.
     * <p>
     * The reason alone determines the {@link Destination}; see {@link #route}.
     */
    public enum Reason {
        /**
         * The value failed to parse with {@code ignore_malformed: true}.
         * Routes to {@link Destination#IGNORE_MALFORMED}.
         */
        MALFORMED,

        /**
         * A {@code multi_value: false} field received a second value in the same document and the
         * field is configured with {@code on_failure: ignore}.
         * Routes to {@link Destination#ON_FAILURE}.
         */
        MULTI_VALUE_VIOLATION,

        /**
         * The field uses synthetic source fallback mode ({@link FieldMapper.SyntheticSourceMode#FALLBACK})
         * and cannot reconstruct its value purely from doc values.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        SYNTHETIC_FALLBACK,

        /**
         * The field or object is configured with {@code source_keep: all}.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        SOURCE_KEEP_ALL,

        /**
         * The field is inside an array scope on a parent with {@code source_keep: arrays}, and the
         * mapper does not natively parse arrays.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        SOURCE_KEEP_ARRAYS_IN_ARRAY,

        /**
         * The field is a {@code copy_to} destination (and the parse is not itself a copy-to traversal).
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        COPY_TO_DESTINATION,

        /**
         * The field is unmapped under a parent with {@code dynamic: false}.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        DYNAMIC_DISABLED,

        /**
         * The field is unmapped under a parent with {@code dynamic: runtime}.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        DYNAMIC_RUNTIME,

        /**
         * The field is inside a disabled object mapper ({@code enabled: false}).
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        OBJECT_DISABLED,

        /**
         * A dynamic field was not indexed because the index's total field count would exceed
         * {@code index.mapping.total_fields.limit}.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        FIELD_LIMIT_EXCEEDED,

        /**
         * A dynamic field was not indexed because the field's name exceeds
         * {@code index.mapping.field_name_length.limit}.
         * Routes to {@link Destination#IGNORED_SOURCE}.
         */
        FIELD_NAME_TOO_LONG;
    }

    // -------------------------------------------------------------------------
    // FieldContext — data record for routing decisions
    // -------------------------------------------------------------------------

    /**
     * A plain-data snapshot of the state needed to decide whether and why a field's XContent should
     * be pre-captured to {@code _ignored_source} before the field mapper runs.
     * <p>
     * All fields are primitive values or enums extracted from the live {@link DocumentParserContext}
     * and {@link FieldMapper} at the point of the routing decision. Because the record holds no live
     * objects, {@link FallbackStorageRouter#resolvePrecaptureReason} can be unit-tested by
     * constructing {@code FieldContext} values directly, without needing a real mapping, parser, or
     * document context.
     * <p>
     * Use the factory methods {@link #forField} and {@link #forArrayElements} to build instances from
     * live objects in production code.
     */
    public record FieldContext(
        /** True when {@link DocumentParserContext#canAddIgnoredField()} returns true. */
        boolean canAddIgnoredField,
        /**
         * True when the mapper stores arrays natively via sidecar offsets or ordered binary doc values.
         * When true, the field reconstructs its arrays from its own doc values and must not be
         * diverted to {@code _ignored_source}.
         */
        boolean storesArraysNatively,
        /**
         * True when the field uses {@link FieldMapper.SyntheticSourceMode#FALLBACK} and cannot
         * reconstruct its value purely from doc values, or when an object mapper's {@code source_keep}
         * setting implies that array elements must be pre-captured.
         */
        boolean syntheticFallback,
        /** The effective {@link Mapper.SourceKeepMode} for this field or object. */
        Mapper.SourceKeepMode sourceKeepMode,
        /** True when the mapper enforces a single-value constraint ({@code multi_value: false}). */
        boolean singleValueEnforced,
        /**
         * The mapper's {@code on_failure} behavior for {@code multi_value: false} violations.
         * Only meaningful when {@link #singleValueEnforced} is true.
         */
        FieldMapper.DocValuesParameter.Values.OnFailure onFailureBehavior,
        /**
         * True when the mapper natively handles arrays in its parse method
         * ({@link FieldMapper#parsesArrayValue()} returns true). When true, the ARRAYS-mode
         * pre-capture branch is skipped.
         */
        boolean parsesArrayValue,
        /**
         * True when the current parse position is inside an array scope, i.e.
         * {@link DocumentParserContext#inArrayScope()} would return true.
         */
        boolean inArrayScope,
        /** True when the current parse is itself a copy-to traversal. */
        boolean isWithinCopyTo,
        /** True when the field's full path is a registered copy-to destination in this document. */
        boolean isCopyToDestinationField
    ) {

        /**
         * Builds a {@link FieldContext} for the regular field parse path
         * ({@code parseObjectOrField} in {@link DocumentParser}).
         * <p>
         * The effective {@link Mapper.SourceKeepMode} is resolved from the mapper's own setting,
         * falling back to the index-level default when the mapper has no explicit preference.
         *
         * @param ctx             the current document parsing context
         * @param mapper          the field mapper about to be parsed
         * @param parsesArrayValue whether the mapper natively handles array values
         */
        public static FieldContext forField(DocumentParserContext ctx, FieldMapper mapper, boolean parsesArrayValue) {
            Mapper.SourceKeepMode mode = mapper.sourceKeepMode().isPresent()
                ? mapper.sourceKeepMode().get()
                : ctx.sourceKeepModeFromIndexSettings();
            return new FieldContext(
                ctx.canAddIgnoredField(),
                false,
                mapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK,
                mode,
                mapper.isSingleValueEnforced(),
                mapper.onFailureBehavior(),
                parsesArrayValue,
                ctx.inArrayScope(),
                ctx.isWithinCopyTo(),
                ctx.isCopyToDestinationField(mapper.fullPath())
            );
        }

        /**
         * Builds a {@link FieldContext} for the array elements path
         * ({@code parseArrayElements} in {@link DocumentParser}).
         * <p>
         * Handles both {@link FieldMapper} and {@link ObjectMapper} mappers. The {@code fullPath}
         * parameter must be the complete dot-separated field path for copy-to destination lookup.
         *
         * @param ctx      the current document parsing context
         * @param mapper   the mapper for the field or object being parsed, or {@code null} if unmapped
         * @param fullPath the full dot-separated field path
         */
        public static FieldContext forArrayElements(DocumentParserContext ctx, Mapper mapper, String fullPath) {
            boolean storesArraysNatively = mapper != null && (mapper.supportStoringArrayOffsets() || mapper.storesArrayValuesInOrder());
            Mapper.SourceKeepMode mode = Mapper.SourceKeepMode.NONE;
            boolean syntheticFallback = false;
            if (mapper instanceof ObjectMapper objectMapper) {
                mode = objectMapper.sourceKeepMode().isPresent()
                    ? objectMapper.sourceKeepMode().get()
                    : ctx.sourceKeepModeFromIndexSettings();
                // An object with source_keep:all or source_keep:arrays (but not nested) must have its
                // array elements pre-captured, since object content cannot be reconstructed field-by-field
                // from doc values when any sub-field is missing from the mapping or uses fallback mode.
                syntheticFallback = mode == Mapper.SourceKeepMode.ALL
                    || (mode == Mapper.SourceKeepMode.ARRAYS && objectMapper instanceof NestedObjectMapper == false);
            } else if (mapper instanceof FieldMapper fieldMapper) {
                mode = fieldMapper.sourceKeepMode().isPresent()
                    ? fieldMapper.sourceKeepMode().get()
                    : ctx.sourceKeepModeFromIndexSettings();
                syntheticFallback = fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK;
            }
            return new FieldContext(
                ctx.canAddIgnoredField(),
                storesArraysNatively,
                syntheticFallback,
                mode,
                false,
                FieldMapper.DocValuesParameter.Values.OnFailure.FAIL,
                false,
                true, // by definition, parseArrayElements is called within an array
                ctx.isWithinCopyTo(),
                ctx.isCopyToDestinationField(fullPath)
            );
        }
    }

    // -------------------------------------------------------------------------
    // Routing decision (pure, no I/O)
    // -------------------------------------------------------------------------

    /**
     * Maps a {@link Reason} to its {@link Destination}.
     * This is the single place in the codebase that defines the reason → destination mapping.
     */
    public static Destination route(Reason reason) {
        return switch (reason) {
            case MALFORMED -> Destination.IGNORE_MALFORMED;
            case MULTI_VALUE_VIOLATION -> Destination.ON_FAILURE;
            case SYNTHETIC_FALLBACK, SOURCE_KEEP_ALL, SOURCE_KEEP_ARRAYS_IN_ARRAY, COPY_TO_DESTINATION, DYNAMIC_DISABLED, DYNAMIC_RUNTIME,
                OBJECT_DISABLED, FIELD_LIMIT_EXCEEDED, FIELD_NAME_TOO_LONG -> Destination.IGNORED_SOURCE;
        };
    }

    /**
     * Decides whether and why a field's XContent should be pre-captured to {@code _ignored_source}
     * before the field mapper runs, based solely on the plain-data {@link FieldContext} snapshot.
     * <p>
     * Returns the {@link Reason} to use for the pre-capture write, or {@link Optional#empty()} if
     * no pre-capture is needed. The returned reason is guaranteed to route to
     * {@link Destination#IGNORED_SOURCE}.
     * <p>
     * Pre-capture is intentionally skipped for fields that redirect {@code multi_value: false}
     * cardinality violations to {@code ._on_failure}: a duplicate value must land in exactly one
     * storage location, and the {@code on_failure: ignore} path already handles that write.
     *
     * @param fc a snapshot of all state needed for the routing decision; create via
     *           {@link FieldContext#forField} or {@link FieldContext#forArrayElements}
     * @return the pre-capture {@link Reason}, or empty if no pre-capture is required
     */
    public static Optional<Reason> resolvePrecaptureReason(FieldContext fc) {
        if (fc.canAddIgnoredField() == false || fc.storesArraysNatively()) {
            return Optional.empty();
        }
        // A multi_value=false field with on_failure=ignore writes its extra values to ._on_failure.
        // Routing it through the pre-capture path would write the first (accepted) occurrence to
        // _ignored_source AND index it normally — double-storing it.
        if (fc.singleValueEnforced() && fc.onFailureBehavior() == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
            return Optional.empty();
        }
        if (fc.isCopyToDestinationField() && fc.isWithinCopyTo() == false) {
            return Optional.of(Reason.COPY_TO_DESTINATION);
        }
        if (fc.syntheticFallback()) {
            return Optional.of(Reason.SYNTHETIC_FALLBACK);
        }
        if (fc.sourceKeepMode() == Mapper.SourceKeepMode.ALL) {
            return Optional.of(Reason.SOURCE_KEEP_ALL);
        }
        if (fc.sourceKeepMode() == Mapper.SourceKeepMode.ARRAYS && fc.inArrayScope() && fc.parsesArrayValue() == false) {
            return Optional.of(Reason.SOURCE_KEEP_ARRAYS_IN_ARRAY);
        }
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // _ignored_source writes
    // -------------------------------------------------------------------------

    /**
     * Encodes the current parser token and immediately writes it to {@code _ignored_source} for
     * {@code fieldPath}, which must be a child field of {@code context.parent()}.
     * <p>
     * Uses {@link IgnoredSourceFieldMapper.NameValue#fromContext} to compute the correct parent
     * offset, including the special case where the parent is the document root.
     *
     * @return {@code true} if the value was written; {@code false} if
     *         {@link DocumentParserContext#canAddIgnoredField()} returned {@code false}
     */
    public static boolean writeToIgnoredSource(DocumentParserContext context, String fieldPath, Reason reason) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        context.addIgnoredField(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, context.encodeFlattenedToken()));
        return true;
    }

    /**
     * Encodes the current parser token and immediately writes {@code context.parent()} to
     * {@code _ignored_source}. Use this when {@code context.parent()} is itself the field being
     * stored (e.g. a disabled object mapper), not a parent container of the field.
     * <p>
     * Computes the correct {@code parentOffset} as
     * {@code parent.fullPath().lastIndexOf(parent.leafName())}, which resolves to {@code 0} for
     * top-level objects and to the index after the last dot for nested objects.
     *
     * @return {@code true} if the value was written; {@code false} if
     *         {@link DocumentParserContext#canAddIgnoredField()} returned {@code false}
     */
    public static boolean writeParentToIgnoredSource(DocumentParserContext context, Reason reason) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        ObjectMapper parent = context.parent();
        int parentOffset = parent.fullPath().lastIndexOf(parent.leafName());
        context.addIgnoredField(
            new IgnoredSourceFieldMapper.NameValue(parent.fullPath(), parentOffset, context.encodeFlattenedToken(), context.doc())
        );
        return true;
    }

    /**
     * Pre-captures the current parser position to {@code _ignored_source} for {@code fieldPath}
     * before the field is parsed, so the original XContent is retained regardless of what the mapper
     * indexes. {@code fieldPath} must be a child field of {@code context.parent()}.
     * <p>
     * If {@link DocumentParserContext#canAddIgnoredField()} returns {@code false}, the context is
     * returned unchanged and no pre-capture is performed.
     *
     * @return the (possibly new, wrapped) {@link DocumentParserContext} for subsequent parsing
     */
    public static DocumentParserContext preCaptureToIgnoredSource(DocumentParserContext context, String fieldPath, Reason reason)
        throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        return context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, null));
    }

    /**
     * Pre-captures the current parser position to {@code _ignored_source} for
     * {@code context.parent()}, before the object's fields are parsed. Use this when
     * {@code context.parent()} is itself the object being captured (e.g. for {@code source_keep}
     * on the object), not a parent container of it.
     * <p>
     * If {@link DocumentParserContext#canAddIgnoredField()} returns {@code false}, the context is
     * returned unchanged and no pre-capture is performed.
     *
     * @return the (possibly new, wrapped) {@link DocumentParserContext} for subsequent parsing
     */
    public static DocumentParserContext preCaptureParentToIgnoredSource(DocumentParserContext context, Reason reason) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        ObjectMapper parent = context.parent();
        int parentOffset = parent.fullPath().lastIndexOf(parent.leafName());
        return context.addIgnoredFieldFromContext(
            new IgnoredSourceFieldMapper.NameValue(parent.fullPath(), parentOffset, null, context.doc())
        );
    }

    // -------------------------------------------------------------------------
    // Immediate write for IGNORE_MALFORMED and ON_FAILURE
    // -------------------------------------------------------------------------

    /**
     * Encodes the current parser token and immediately writes it to the appropriate fallback
     * storage destination for the given reason.
     * <p>
     * This method handles {@link Reason#MALFORMED} and {@link Reason#MULTI_VALUE_VIOLATION}.
     * All {@link Destination#IGNORED_SOURCE} writes must use {@link #writeToIgnoredSource},
     * {@link #writeParentToIgnoredSource}, or {@link #preCaptureToIgnoredSource} instead.
     *
     * @param context   the current document parsing context; the encoded value is written to its
     *                  Lucene document
     * @param fieldPath the full path of the field whose fallback column the value is stored under
     * @param reason    why this value is being redirected to fallback storage
     * @param parser    positioned at the value to encode and store
     * @throws IOException if encoding or writing fails
     * @throws IllegalArgumentException if {@code reason} routes to {@link Destination#IGNORED_SOURCE}
     */
    public static void write(DocumentParserContext context, String fieldPath, Reason reason, XContentParser parser) throws IOException {
        switch (route(reason)) {
            case IGNORE_MALFORMED -> IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fieldPath, parser);
            case ON_FAILURE -> {
                // Stored source already retains the offending value verbatim; only synthetic source
                // (and columnar_stored, which reconstructs its per-document source the same way)
                // need the value captured in the failure column to survive reconstruction.
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeValueForOnFailureIgnore(context, fieldPath, parser);
                }
            }
            case IGNORED_SOURCE -> throw new IllegalArgumentException(
                "Use writeToIgnoredSource or preCaptureToIgnoredSource for IGNORED_SOURCE reason: " + reason
            );
        }
    }

}
