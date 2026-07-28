/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Optional;

/**
 * Central routing for field values that cannot be indexed normally.
 * Redirects to {@link Destination#IGNORED_SOURCE} ({@code _ignored_source}),
 * {@link Destination#IGNORE_MALFORMED} ({@code ._ignore_malformed}), or
 * {@link Destination#ON_FAILURE} ({@code ._on_failure}) based on a {@link Reason}.
 */
public final class FallbackStorageRouter {

    private FallbackStorageRouter() {}

    /** The storage destination for a field value that cannot be indexed normally. */
    public enum Destination {
        /** {@code _ignored_source} metadata field; used for synthetic source reconstruction. */
        IGNORED_SOURCE,
        /** Per-field {@code ._ignore_malformed} column; used with {@code ignore_malformed: true}. */
        IGNORE_MALFORMED,
        /** Per-field {@code ._on_failure} column; used with {@code multi_value: false, on_failure: ignore}. */
        ON_FAILURE;
    }

    /**
     * Why a field value is being redirected to fallback storage.
     * The reason alone determines the {@link Destination}; see {@link #route}.
     */
    public enum Reason {
        /** Value failed to parse with {@code ignore_malformed: true}. Routes to {@link Destination#IGNORE_MALFORMED}. */
        MALFORMED,
        /** {@code multi_value: false} field received a duplicate with {@code on_failure: ignore}.
         * Routes to {@link Destination#ON_FAILURE}. */
        MULTI_VALUE_VIOLATION,
        /** Field uses {@link FieldMapper.SyntheticSourceMode#FALLBACK}. Routes to {@link Destination#IGNORED_SOURCE}. */
        SYNTHETIC_FALLBACK,
        /** Field or object has {@code source_keep: all}. Routes to {@link Destination#IGNORED_SOURCE}. */
        SOURCE_KEEP_ALL,
        /** Field is in an array with {@code source_keep: arrays} and the mapper doesn't handle arrays natively.
         *  Routes to {@link Destination#IGNORED_SOURCE}. */
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

    /**
     * Plain-data snapshot used by {@link #resolvePrecaptureReason} to decide whether pre-capture is needed.
     * Build with {@link #forField} or {@link #forArrayElements}.
     */
    public record FieldContext(
        boolean canAddIgnoredField,
        /** True when the mapper reconstructs arrays from its own doc values (sidecar offsets or ordered BDV). */
        boolean storesArraysNatively,
        /** True when the mapper uses {@link FieldMapper.SyntheticSourceMode#FALLBACK},
         * or when an object's {@code source_keep} forces pre-capture of array elements. */
        boolean syntheticFallback,
        Mapper.SourceKeepMode sourceKeepMode,
        /** True when the mapper handles arrays natively in its parse method ({@link FieldMapper#parsesArrayValue()}). */
        boolean parsesArrayValue,
        boolean inArrayScope,
        boolean isWithinCopyTo,
        boolean isCopyToDestinationField
    ) {

        /** Builds a {@link FieldContext} for the regular field parse path ({@code parseObjectOrField}). */
        public static FieldContext forField(DocumentParserContext ctx, FieldMapper mapper) {
            Mapper.SourceKeepMode mode = mapper.sourceKeepMode().isPresent()
                ? mapper.sourceKeepMode().get()
                : ctx.sourceKeepModeFromIndexSettings();
            return new FieldContext(
                ctx.canAddIgnoredField(),
                false,
                mapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK,
                mode,
                mapper.parsesArrayValue(),
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

    /** Maps a {@link Reason} to its {@link Destination}. */
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

    /**
     * Sets up pre-capture in {@code _ignored_source} if needed and returns the context to pass to {@link FieldMapper#parse}.
     * Must be followed by {@link #postParse} after the parse call.
     */
    public static DocumentParserContext preCaptureIfNeeded(DocumentParserContext context, FieldMapper fieldMapper) throws IOException {
        FieldContext fc = FieldContext.forField(context, fieldMapper);
        if (resolvePrecaptureReason(fc).isPresent()) {
            return context.addPendingPreCapture(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldMapper.fullPath(), null));
        }
        return context;
    }

    /**
     * Commits or discards the pending pre-capture based on {@code result}, and routes
     * {@link ParseResult.MultiValueViolation} to {@code ._on_failure}. Call after {@link FieldMapper#parse}.
     */
    public static void postParse(DocumentParserContext context, ParseResult result, FieldMapper fieldMapper) throws IOException {
        String fieldPath = fieldMapper.fullPath();
        boolean precaptured = context.hasPendingPreCapture(fieldPath);
        switch (result) {
            case ParseResult.MultiValueViolation mvv -> {
                if (precaptured) context.discardPendingPreCapture(fieldPath);
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeEncoded(context, fieldPath, mvv.capturedValue());
                }
            }
            case ParseResult.Malformed() -> {
                if (precaptured) {
                    if (fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK) {
                        context.commitPendingPreCapture(fieldPath);
                    } else {
                        context.discardPendingPreCapture(fieldPath);
                    }
                }
            }
            case ParseResult.Indexed ignored -> {
                if (precaptured) context.commitPendingPreCapture(fieldPath);
            }
        }
    }

    /**
     * Like {@link #write(DocumentParserContext, String, Reason)} but uses a pre-built {@link XContentBuilder}
     * instead of the current parser token (e.g. when the mapper captured via {@code CopyingXContentParser}).
     */
    public static boolean write(DocumentParserContext context, String fieldPath, Reason reason, XContentBuilder builder)
        throws IOException {
        return switch (route(reason)) {
            case IGNORED_SOURCE -> writeToIgnoredSource(context, fieldPath, builder);
            case IGNORE_MALFORMED -> {
                IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fieldPath, builder);
                yield true;
            }
            case ON_FAILURE -> {
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeEncoded(context, fieldPath, XContentDataHelper.encodeXContentBuilder(builder));
                }
                yield true;
            }
        };
    }

    /**
     * Writes the current parser token to the fallback destination for {@code fieldPath}.
     * Returns {@code false} only for {@link Destination#IGNORED_SOURCE} when
     * {@link DocumentParserContext#canAddIgnoredField()} is false; {@code true} otherwise.
     */
    public static boolean write(DocumentParserContext context, String fieldPath, Reason reason) throws IOException {
        return switch (route(reason)) {
            case IGNORED_SOURCE -> writeToIgnoredSource(context, fieldPath);
            case IGNORE_MALFORMED -> {
                IgnoreMalformedStoredValues.storeMalformedValueForSyntheticSource(context, fieldPath, context.parser());
                yield true;
            }
            case ON_FAILURE -> {
                if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                    OnFailureStoredValues.storeValueForOnFailureIgnore(context, fieldPath, context.parser());
                }
                yield true;
            }
        };
    }

    /**
     * Like {@link #write(DocumentParserContext, String, Reason)} but stores {@code context.parent()}
     * as the captured entity (e.g. a disabled object), not a child field.
     */
    public static boolean writeParent(DocumentParserContext context, Reason reason) throws IOException {
        if (route(reason) == Destination.IGNORED_SOURCE) {
            return writeParentToIgnoredSource(context);
        }
        return write(context, context.parent().fullPath(), reason);
    }

    /**
     * Pre-captures the current parser position for {@code fieldPath} so it can be reconstructed in
     * {@code _ignored_source}. Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
     */
    public static DocumentParserContext preCapture(DocumentParserContext context, String fieldPath) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        return context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, null));
    }

    /**
     * Like {@link #preCapture(DocumentParserContext, String)} but captures {@code context.parent()}
     * as the entity (e.g. a disabled object), not a child field.
     * Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
     */
    public static DocumentParserContext preCaptureParent(DocumentParserContext context) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        ObjectMapper parent = context.parent();
        String parentPath = parent.fullPath();
        int parentOffset = parentPath.lastIndexOf(parent.leafName());
        return context.addIgnoredFieldFromContext(new IgnoredSourceFieldMapper.NameValue(parentPath, parentOffset, null, context.doc()));
    }

    private static boolean writeToIgnoredSource(DocumentParserContext context, String fieldPath) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        context.addIgnoredField(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, context.encodeFlattenedToken()));
        return true;
    }

    private static boolean writeToIgnoredSource(DocumentParserContext context, String fieldPath, XContentBuilder builder)
        throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        context.addIgnoredField(
            IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, XContentDataHelper.encodeXContentBuilder(builder))
        );
        return true;
    }

    private static boolean writeParentToIgnoredSource(DocumentParserContext context) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return false;
        }
        ObjectMapper parent = context.parent();
        String parentPath = parent.fullPath();
        int parentOffset = parentPath.lastIndexOf(parent.leafName());
        context.addIgnoredField(
            new IgnoredSourceFieldMapper.NameValue(parentPath, parentOffset, context.encodeFlattenedToken(), context.doc())
        );
        return true;
    }

}
