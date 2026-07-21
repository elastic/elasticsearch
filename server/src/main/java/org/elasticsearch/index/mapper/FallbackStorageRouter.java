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
import java.util.Optional;

/**
 * Single owner of fallback storage routing decisions.
 * <p>
 * When a field value cannot be indexed normally it is diverted to one of three destinations:
 * {@link Destination#IGNORED_SOURCE} ({@code _ignored_source}),
 * {@link Destination#IGNORE_MALFORMED} ({@code ._ignore_malformed}), or
 * {@link Destination#ON_FAILURE} ({@code ._on_failure}).
 * Call {@link #resolvePrecaptureReason} for pre-capture decisions and {@link #write} for
 * immediate malformed/cardinality writes.
 */
public final class FallbackStorageRouter {

    private FallbackStorageRouter() {}

    // -------------------------------------------------------------------------
    // Destination enum — WHERE a redirected value is stored
    // -------------------------------------------------------------------------

    /** The storage destination for a field value that cannot be indexed normally. */
    public enum Destination {
        /** {@code _ignored_source} metadata field; used for synthetic source reconstruction. */
        IGNORED_SOURCE,
        /** Per-field {@code ._ignore_malformed} column; used with {@code ignore_malformed: true}. */
        IGNORE_MALFORMED,
        /** Per-field {@code ._on_failure} column; used with {@code multi_value: false, on_failure: ignore}. */
        ON_FAILURE;
    }

    // -------------------------------------------------------------------------
    // Reason enum — WHY a value is being redirected to fallback storage
    // -------------------------------------------------------------------------

    /**
     * Why a field value is being redirected to fallback storage.
     * The reason alone determines the {@link Destination}; see {@link #route}.
     */
    public enum Reason {
        /** Value failed to parse with {@code ignore_malformed: true}. Routes to {@link Destination#IGNORE_MALFORMED}. */
        MALFORMED,
        /** {@code multi_value: false} field received a duplicate with {@code on_failure: ignore}. Routes to {@link Destination#ON_FAILURE}. */
        MULTI_VALUE_VIOLATION,
        /** Field uses {@link FieldMapper.SyntheticSourceMode#FALLBACK}. Routes to {@link Destination#IGNORED_SOURCE}. */
        SYNTHETIC_FALLBACK,
        /** Field or object has {@code source_keep: all}. Routes to {@link Destination#IGNORED_SOURCE}. */
        SOURCE_KEEP_ALL,
        /** Field is in an array with {@code source_keep: arrays} and the mapper doesn't handle arrays natively. Routes to {@link Destination#IGNORED_SOURCE}. */
        SOURCE_KEEP_ARRAYS_IN_ARRAY,
        /** Field is a {@code copy_to} destination (not within a copy-to traversal). Routes to {@link Destination#IGNORED_SOURCE}. */
        COPY_TO_DESTINATION,
        /** Field is unmapped under {@code dynamic: false}. Routes to {@link Destination#IGNORED_SOURCE}. */
        DYNAMIC_DISABLED,
        /** Field is unmapped under {@code dynamic: runtime}. Routes to {@link Destination#IGNORED_SOURCE}. */
        DYNAMIC_RUNTIME,
        /** Field is inside a disabled object ({@code enabled: false}). Routes to {@link Destination#IGNORED_SOURCE}. */
        OBJECT_DISABLED,
        /** Dynamic field skipped because the index field-count limit would be exceeded. Routes to {@link Destination#IGNORED_SOURCE}. */
        FIELD_LIMIT_EXCEEDED,
        /** Dynamic field skipped because its name exceeds the field-name length limit. Routes to {@link Destination#IGNORED_SOURCE}. */
        FIELD_NAME_TOO_LONG;
    }

    // -------------------------------------------------------------------------
    // FieldContext — plain-data snapshot for pre-capture routing decisions
    // -------------------------------------------------------------------------

    /**
     * Plain-data snapshot of the state needed to decide whether and why a field's XContent should be
     * pre-captured to {@code _ignored_source} before parsing. Holding only primitives and enums means
     * {@link #resolvePrecaptureReason} has no live-object dependencies and can be tested by
     * constructing records directly. Use {@link #forField} or {@link #forArrayElements} in production.
     */
    public record FieldContext(
        boolean canAddIgnoredField,
        /** True when the mapper reconstructs arrays from its own doc values (sidecar offsets or ordered BDV). */
        boolean storesArraysNatively,
        /** True when the mapper uses {@link FieldMapper.SyntheticSourceMode#FALLBACK}, or when an object's {@code source_keep} forces pre-capture of array elements. */
        boolean syntheticFallback,
        Mapper.SourceKeepMode sourceKeepMode,
        /** True when the mapper handles arrays natively in its parse method ({@link FieldMapper#parsesArrayValue()}). */
        boolean parsesArrayValue,
        boolean inArrayScope,
        boolean isWithinCopyTo,
        boolean isCopyToDestinationField
    ) {

        /** Builds a {@link FieldContext} for the regular field parse path ({@code parseObjectOrField}). */
        public static FieldContext forField(DocumentParserContext ctx, FieldMapper mapper, boolean parsesArrayValue) {
            Mapper.SourceKeepMode mode = mapper.sourceKeepMode().isPresent()
                ? mapper.sourceKeepMode().get()
                : ctx.sourceKeepModeFromIndexSettings();
            return new FieldContext(
                ctx.canAddIgnoredField(),
                false,
                mapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK,
                mode,
                parsesArrayValue,
                ctx.inArrayScope(),
                ctx.isWithinCopyTo(),
                ctx.isCopyToDestinationField(mapper.fullPath())
            );
        }

        /**
         * Builds a {@link FieldContext} for the array elements path ({@code parseArrayElements}).
         * Handles both {@link FieldMapper} and {@link ObjectMapper}.
         */
        public static FieldContext forArrayElements(DocumentParserContext ctx, Mapper mapper, String fullPath) {
            boolean storesArraysNatively = mapper != null && (mapper.supportStoringArrayOffsets() || mapper.storesArrayValuesInOrder());
            Mapper.SourceKeepMode mode = Mapper.SourceKeepMode.NONE;
            boolean syntheticFallback = false;
            if (mapper instanceof ObjectMapper objectMapper) {
                mode = objectMapper.sourceKeepMode().isPresent()
                    ? objectMapper.sourceKeepMode().get()
                    : ctx.sourceKeepModeFromIndexSettings();
                // Objects with source_keep:all or source_keep:arrays (non-nested) must pre-capture array
                // elements because object content cannot be reconstructed field-by-field from doc values.
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
                true, // by definition, parseArrayElements is called within an array
                ctx.isWithinCopyTo(),
                ctx.isCopyToDestinationField(fullPath)
            );
        }
    }

    // -------------------------------------------------------------------------
    // Routing decisions (pure, no I/O)
    // -------------------------------------------------------------------------

    /** Maps a {@link Reason} to its {@link Destination}. Single source of truth for this mapping. */
    public static Destination route(Reason reason) {
        return switch (reason) {
            case MALFORMED -> Destination.IGNORE_MALFORMED;
            case MULTI_VALUE_VIOLATION -> Destination.ON_FAILURE;
            case SYNTHETIC_FALLBACK, SOURCE_KEEP_ALL, SOURCE_KEEP_ARRAYS_IN_ARRAY, COPY_TO_DESTINATION, DYNAMIC_DISABLED, DYNAMIC_RUNTIME,
                OBJECT_DISABLED, FIELD_LIMIT_EXCEEDED, FIELD_NAME_TOO_LONG -> Destination.IGNORED_SOURCE;
        };
    }

    /**
     * Returns the {@link Reason} to use when pre-capturing a field's XContent to {@code _ignored_source}
     * before the mapper runs, or {@link Optional#empty()} if no pre-capture is needed.
     * <p>
     * Pre-capture is skipped for {@code multi_value=false, on_failure=ignore} fields: their extra values
     * go to {@code ._on_failure} and pre-capturing would double-store the first accepted value.
     */
    public static Optional<Reason> resolvePrecaptureReason(FieldContext fc) {
        if (fc.canAddIgnoredField() == false || fc.storesArraysNatively()) {
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
    // Central parse entry point
    // -------------------------------------------------------------------------

    /**
     * Parses a field value through the fallback storage pipeline.
     * <p>
     * Tentatively pre-captures the value when the field participates in any {@code _ignored_source} fallback
     * path, then delegates to the mapper. After the mapper returns, the pre-capture is either committed
     * (successful index), discarded (malformed — mapper already wrote to {@code ._ignore_malformed}), or
     * discarded and re-routed to {@code ._on_failure} (multi-value violation).
     *
     * @return the outcome of parsing the field value
     */
    public static ParseResult parseField(DocumentParserContext context, FieldMapper fieldMapper) throws IOException {
        FieldContext fc = FieldContext.forField(context, fieldMapper, fieldMapper.parsesArrayValue());

        // Tentative pre-capture when the field participates in any ignored-source fallback path
        boolean precaptured = false;
        DocumentParserContext parseCtx = context;
        if (resolvePrecaptureReason(fc).isPresent()) {
            parseCtx = context.addPendingPreCapture(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldMapper.fullPath(), null));
            precaptured = true;
        }

        boolean wasAlreadyIgnored = context.getIgnoredFields().contains(fieldMapper.fullPath());
        fieldMapper.parse(parseCtx);

        // Multi-value violation: stash populated by enforceSingleValue — drain and route to ._on_failure
        BytesRef mvvStash = context.takePendingMultiValueViolation(fieldMapper.fullPath());
        if (mvvStash != null) {
            if (precaptured) context.discardPendingPreCapture(fieldMapper.fullPath());
            if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                OnFailureStoredValues.storeEncoded(context, fieldMapper.fullPath(), mvvStash);
            }
            return new ParseResult.MultiValueViolation(mvvStash);
        }

        // Malformed: mapper called addIgnoredField and already wrote to ._ignore_malformed
        if (wasAlreadyIgnored == false && context.getIgnoredFields().contains(fieldMapper.fullPath())) {
            if (precaptured) context.discardPendingPreCapture(fieldMapper.fullPath());
            return new ParseResult.Malformed(null);
        }

        // Successfully indexed: commit the tentative pre-capture to _ignored_source
        if (precaptured) context.commitPendingPreCapture(fieldMapper.fullPath());
        return new ParseResult.Indexed();
    }

    // -------------------------------------------------------------------------
    // _ignored_source writes
    // -------------------------------------------------------------------------

    /**
     * Encodes the current parser token and writes it to {@code _ignored_source} for {@code fieldPath}
     * (a child of {@code context.parent()}).
     *
     * @return {@code true} if written; {@code false} if {@link DocumentParserContext#canAddIgnoredField()} is false
     */
    public static boolean writeToIgnoredSource(DocumentParserContext context, String fieldPath, Reason reason) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        context.addIgnoredField(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, context.encodeFlattenedToken()));
        return true;
    }

    /**
     * Encodes the current parser token and writes {@code context.parent()} to {@code _ignored_source}.
     * Use when {@code context.parent()} is the field being stored (e.g. a disabled object), not a container.
     *
     * @return {@code true} if written; {@code false} if {@link DocumentParserContext#canAddIgnoredField()} is false
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
     * (a child of {@code context.parent()}) before the field is parsed.
     * Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
     */
    public static DocumentParserContext preCaptureToIgnoredSource(DocumentParserContext context, String fieldPath, Reason reason)
        throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        return context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, null));
    }

    /**
     * Pre-captures the current parser position to {@code _ignored_source} for {@code context.parent()}.
     * Use when {@code context.parent()} is the object being captured, not a container.
     * Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
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
     * Writes the current parser token to the appropriate fallback destination for {@link Reason#MALFORMED}
     * or {@link Reason#MULTI_VALUE_VIOLATION}. Throws {@link IllegalArgumentException} if the reason
     * routes to {@link Destination#IGNORED_SOURCE}; use {@link #writeToIgnoredSource} or
     * {@link #preCaptureToIgnoredSource} for those.
     */
    public static void write(DocumentParserContext context, String fieldPath, Reason reason, XContentParser parser) throws IOException {
        switch (route(reason)) {
            case IGNORE_MALFORMED -> IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fieldPath, parser);
            case ON_FAILURE -> {
                // Stored source retains the value verbatim; only synthetic/columnar_stored source needs
                // the failure column to reproduce it during reconstruction.
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
