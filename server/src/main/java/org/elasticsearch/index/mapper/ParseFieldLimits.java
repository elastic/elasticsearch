/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexSettings;

/**
 * Tracks per-field limits during mapping parsing and merging.
 * A single instance is shared across all child contexts within one operation so that
 * counts accumulate correctly across the whole mapper tree.
 */
public class ParseFieldLimits {

    public static final ParseFieldLimits UNLIMITED = new ParseFieldLimits(
        Long.MAX_VALUE,
        Long.MAX_VALUE,
        Long.MAX_VALUE,
        NewFieldsBudget.unlimited(),
        0
    );

    private final long mappingDepthLimit;
    private final long fieldNameLengthLimit;
    private final long nestedFieldsLimit;
    private long nestedFieldsCount;
    private final NewFieldsBudget totalFieldsBudget;

    ParseFieldLimits(
        long mappingDepthLimit,
        long fieldNameLengthLimit,
        long nestedFieldsLimit,
        NewFieldsBudget totalFieldsBudget,
        long initialNestedCount
    ) {
        this.mappingDepthLimit = mappingDepthLimit;
        this.fieldNameLengthLimit = fieldNameLengthLimit;
        this.nestedFieldsLimit = nestedFieldsLimit;
        this.totalFieldsBudget = totalFieldsBudget;
        this.nestedFieldsCount = initialNestedCount;
    }

    /**
     * Builds the {@link ParseFieldLimits} appropriate for the given merge reason and index settings.
     * Recovery re-uses a mapping that was already validated, so no limits are enforced. Auto-updates
     * in drop-mode use per-field name/nested limits but leave total-fields counting to the merge-time
     * budget. All other updates additionally enforce a parse-time total-fields throwing budget.
     */
    public static ParseFieldLimits parseFieldLimits(MapperService.MergeReason reason, IndexSettings indexSettings) {
        if (reason == MapperService.MergeReason.MAPPING_RECOVERY) {
            return ParseFieldLimits.UNLIMITED;
        }
        long depthLimit = indexSettings.getMappingDepthLimit();
        long nameLimit = indexSettings.getMappingFieldNameLengthLimit();
        long nestedLimit = indexSettings.getMappingNestedFieldsLimit();
        if (reason.isAutoUpdate() && indexSettings.isIgnoreDynamicFieldsBeyondLimit()) {
            return new ParseFieldLimits(depthLimit, nameLimit, nestedLimit, NewFieldsBudget.unlimited(), 0);
        }
        long totalLimit = indexSettings.getMappingTotalFieldsLimit();
        return new ParseFieldLimits(depthLimit, nameLimit, nestedLimit, NewFieldsBudget.throwing(totalLimit, totalLimit), 0);
    }

    /**
     * Creates limits for use at merge time, enforcing only the nested fields count.
     * Use when no existing mapper is present (existing nested count is zero).
     */
    public static ParseFieldLimits forMerge(long nestedFieldsLimit) {
        return new ParseFieldLimits(Long.MAX_VALUE, Long.MAX_VALUE, nestedFieldsLimit, NewFieldsBudget.unlimited(), 0);
    }

    /**
     * Creates limits for use at merge time, enforcing nested fields count relative to an existing mapper.
     */
    public static ParseFieldLimits forMerge(long nestedFieldsLimit, long existingNestedCount, NewFieldsBudget totalFieldsBudget) {
        return new ParseFieldLimits(Long.MAX_VALUE, Long.MAX_VALUE, nestedFieldsLimit, totalFieldsBudget, existingNestedCount);
    }

    /**
     * Wraps an existing {@link NewFieldsBudget} with no nested or name limits, for backward-compatible
     * construction of a {@link MapperMergeContext} that does not enforce nested field counts.
     */
    public static ParseFieldLimits withBudget(NewFieldsBudget budget) {
        return new ParseFieldLimits(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, budget, 0);
    }

    /**
     * Creates limits for use when dynamically adding mappings during document parsing.
     */
    static ParseFieldLimits forDocument(MappingLookup mappingLookup, IndexSettings indexSettings) {
        long totalFieldsLimit = indexSettings.getMappingTotalFieldsLimit();
        long remaining = mappingLookup.remainingFieldsUntilLimit(totalFieldsLimit);
        NewFieldsBudget budget = indexSettings.isIgnoreDynamicFieldsBeyondLimit()
            ? NewFieldsBudget.dropping(remaining)
            : NewFieldsBudget.throwing(remaining, totalFieldsLimit);
        return new ParseFieldLimits(
            indexSettings.getMappingDepthLimit(),
            indexSettings.getMappingFieldNameLengthLimit(),
            indexSettings.getMappingNestedFieldsLimit(),
            budget,
            0
        );
    }

    /**
     * Returns a copy of this ParseFieldLimits with an unlimited total fields budget.
     */
    ParseFieldLimits withoutTotalFieldsBudget() {
        return new ParseFieldLimits(mappingDepthLimit, fieldNameLengthLimit, nestedFieldsLimit, NewFieldsBudget.unlimited(), 0);
    }

    /**
     * Checks that adding a dynamic object at {@code fullPath} would not exceed the object depth limit.
     * Depth is computed by counting dots in the path (each dot represents one nesting level).
     * Called eagerly during document parsing to avoid stack-overflow errors from deeply nested objects.
     */
    void checkObjectDepth(String fullPath) {
        int numDots = 0;
        for (int i = 0; i < fullPath.length(); i++) {
            if (fullPath.charAt(i) == '.') {
                numDots++;
            }
        }
        int depth = numDots + 2;
        if (depth > mappingDepthLimit) {
            throw new MapperParsingException(
                "Limit of mapping depth [" + mappingDepthLimit + "] has been exceeded due to object field [" + fullPath + "]"
            );
        }
    }

    long mappingDepthLimit() {
        return mappingDepthLimit;
    }

    void checkFieldNameLength(String leafName) {
        if (leafName.length() > fieldNameLengthLimit) {
            throw new MapperParsingException(
                "Field name [" + leafName + "] is longer than the limit of [" + fieldNameLengthLimit + "] characters"
            );
        }
    }

    void checkNestedFieldCount() {
        nestedFieldsCount++;
        if (nestedFieldsCount > nestedFieldsLimit) {
            throw new MapperParsingException("Limit of nested fields [" + nestedFieldsLimit + "] has been exceeded");
        }
    }

    boolean hasCapacityForTotalFields(long fieldSize) {
        return totalFieldsBudget.hasCapacityFor(fieldSize);
    }

    boolean decrementTotalFieldsIfPossible(long fieldSize) {
        return totalFieldsBudget.decrementIfPossible(fieldSize);
    }
}
