/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Central fallback routing for field values that cannot be indexed normally.
 * Redirects to {@link Destination#IGNORED_SOURCE} ({@code _ignored_source}),
 * {@link Destination#IGNORE_MALFORMED} ({@code ._ignore_malformed}), or
 * {@link Destination#ON_FAILURE} ({@code ._on_failure}) based on a {@link Reason}.
 */
public final class FallbackPostMapper {

    private FallbackPostMapper() {}

    /** The storage destination for a field value that cannot be indexed normally. */
    enum Destination {
        /** {@code _ignored_source} metadata field; used for synthetic source reconstruction. */
        IGNORED_SOURCE,
        /** Per-field {@code ._ignore_malformed} column; used with {@code ignore_malformed: true} outside strict-columnar modes. */
        IGNORE_MALFORMED,
        /**
         * Per-field {@code ._on_failure} column; used with {@code multi_value: false, on_failure: ignore}, and additionally
         * with {@code ignore_malformed: true} in strict-columnar modes (where malformed values share the column).
         */
        ON_FAILURE;
    }

    /**
     * Why a field value is being redirected to fallback storage.
     * The reason alone determines the {@link Destination}; see {@link #route}.
     */
    public enum Reason {
        /**
         * Value failed to parse with {@code ignore_malformed: true}. Routes to {@link Destination#IGNORE_MALFORMED} outside
         * strict-columnar modes; routes to {@link Destination#ON_FAILURE} in strict-columnar modes so malformed values share the
         * per-field {@code ._on_failure} sidecar column with multi-value violations.
         */
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
    record FieldContext(
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
                // Nested objects natively preserve array structure via Lucene's nested document mechanism;
                // clear ARRAYS mode so SOURCE_KEEP_ARRAYS_IN_ARRAY pre-capture is not triggered.
                if (objectMapper instanceof NestedObjectMapper && mode == Mapper.SourceKeepMode.ARRAYS) {
                    mode = Mapper.SourceKeepMode.NONE;
                }
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

    /**
     * Maps a {@link Reason} to its {@link Destination}.
     * In strict-columnar index modes, {@link Reason#MALFORMED} routes to {@link Destination#ON_FAILURE} so that malformed values share
     * the per-field {@code ._on_failure} sidecar column with multi-value violations, collapsing two fallback columns into one.
     */
    static Destination route(Reason reason, boolean strictColumnar) {
        return switch (reason) {
            case MALFORMED -> strictColumnar ? Destination.ON_FAILURE : Destination.IGNORE_MALFORMED;
            case MULTI_VALUE_VIOLATION -> Destination.ON_FAILURE;
            case SYNTHETIC_FALLBACK, SOURCE_KEEP_ALL, SOURCE_KEEP_ARRAYS_IN_ARRAY, COPY_TO_DESTINATION, DYNAMIC_DISABLED, DYNAMIC_RUNTIME,
                OBJECT_DISABLED, FIELD_LIMIT_EXCEEDED, FIELD_NAME_TOO_LONG -> Destination.IGNORED_SOURCE;
        };
    }

    /**
     * Returns the {@link Reason} to use when pre-capturing a field's XContent to {@code _ignored_source}
     * before the mapper runs, or {@code null} if no pre-capture is needed.
     */
    @Nullable
    static Reason resolvePrecaptureReason(FieldContext fc) {
        if (fc.canAddIgnoredField() == false || fc.storesArraysNatively()) {
            return null;
        }
        if (fc.isCopyToDestinationField() && fc.isWithinCopyTo() == false) {
            return Reason.COPY_TO_DESTINATION;
        }
        if (fc.syntheticFallback()) {
            return Reason.SYNTHETIC_FALLBACK;
        }
        if (fc.sourceKeepMode() == Mapper.SourceKeepMode.ALL) {
            return Reason.SOURCE_KEEP_ALL;
        }
        if (fc.sourceKeepMode() == Mapper.SourceKeepMode.ARRAYS && fc.inArrayScope() && fc.parsesArrayValue() == false) {
            return Reason.SOURCE_KEEP_ARRAYS_IN_ARRAY;
        }
        return null;
    }

    /**
     * The single entry point for parsing a mapped leaf field. When {@link #resolvePrecaptureReason}
     * returns non-null, captures the XContent token to {@code _ignored_source} before delegating to
     * {@link FieldMapper#parse}.
     *
     * @return the context the mapper parsed with. On pre-capture this is a recorded sub-context, and
     *         callers must propagate it to any subsequent {@code copy_to} traversal: the traversal must
     *         not add a second {@code _ignored_source} entry for the destination, and
     *         {@code recordedSource} on this context is what suppresses that.
     */
    public static DocumentParserContext parseField(DocumentParserContext context, FieldMapper fieldMapper) throws IOException {
        String fieldPath = fieldMapper.fullPath();
        DocumentParserContext parseCtx = resolvePrecaptureReason(FieldContext.forField(context, fieldMapper)) != null
            ? context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fieldPath, null))
            : context;

        FieldMapper.ParseResult result = fieldMapper.parse(parseCtx);

        if (result instanceof FieldMapper.ParseResult.MultiValueViolation mvv) {
            // multi_value violations require columnar mode, which disables canAddIgnoredField(), so
            // pre-capture is never active for the same field simultaneously.
            assert parseCtx == context : "multi_value violation on pre-captured field [" + fieldPath + "]";
            if (context.mappingLookup().isSourceSynthetic() || context.mappingLookup().isSourceColumnarStored()) {
                OnFailureStoredValues.storeEncoded(context, fieldPath, mvv.capturedValue());
            }
        }
        return parseCtx;
    }

    /**
     * Like {@link #capture(DocumentParserContext, String, Reason)} but uses a pre-built {@link XContentBuilder}
     * instead of the current parser token (e.g. when the mapper captured via {@code CopyingXContentParser}).
     */
    public static boolean capture(DocumentParserContext context, String fieldPath, Reason reason, XContentBuilder builder)
        throws IOException {
        return switch (route(reason, context.indexSettings().getMode().isStrictColumnar())) {
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
     *
     * Only returns {@code false} for {@link Destination#IGNORED_SOURCE} when
     * {@link DocumentParserContext#canAddIgnoredField()} is false; {@code true} otherwise.
     */
    public static boolean capture(DocumentParserContext context, String fieldPath, Reason reason) throws IOException {
        return switch (route(reason, context.indexSettings().getMode().isStrictColumnar())) {
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
     * Like {@link #capture(DocumentParserContext, String, Reason)} but stores {@code context.parent()}
     * as the captured entity (e.g. a disabled object), not a child field.
     */
    static boolean captureParent(DocumentParserContext context, Reason reason) throws IOException {
        if (route(reason, context.indexSettings().getMode().isStrictColumnar()) == Destination.IGNORED_SOURCE) {
            return writeParentToIgnoredSource(context);
        }
        return capture(context, context.parent().fullPath(), reason);
    }

    /**
     * Pre-captures the current parser position for {@code fullPath} so it can be reconstructed in
     * {@code _ignored_source}. Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
     * Use when no {@link Mapper} is available for the target (e.g. unmapped dynamic fields).
     */
    static DocumentParserContext captureScope(DocumentParserContext context, String fullPath) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        return context.addIgnoredFieldFromContext(IgnoredSourceFieldMapper.NameValue.fromContext(context, fullPath, null));
    }

    /**
     * Pre-captures the current parser position for {@code target} so it can be reconstructed in
     * {@code _ignored_source}. The router derives the path offset from {@code target.leafName()},
     * so callers need not perform that arithmetic.
     * Returns the context unchanged if {@link DocumentParserContext#canAddIgnoredField()} is false.
     */
    static DocumentParserContext captureScope(DocumentParserContext context, Mapper target) throws IOException {
        if (context.canAddIgnoredField() == false) {
            return context;
        }
        String path = target.fullPath();
        int offset = path.lastIndexOf(target.leafName());
        return context.addIgnoredFieldFromContext(new IgnoredSourceFieldMapper.NameValue(path, offset, null, context.doc()));
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
