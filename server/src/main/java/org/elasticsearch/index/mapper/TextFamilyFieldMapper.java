/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.Map;

/**
 * This class is meant to contain common functionality that is needed by the Text family of field mappers. Namely
 * {@link TextFieldMapper} and anything strongly related to it.
 */
public abstract class TextFamilyFieldMapper extends FieldMapper {

    protected final IndexVersion indexCreatedVersion;
    protected final boolean isSyntheticSourceEnabled;
    protected final boolean isWithinMultiField;

    protected TextFamilyFieldMapper(
        String name,
        IndexVersion indexCreatedVersion,
        boolean isSyntheticSourceEnabled,
        boolean isWithinMultiField,
        MappedFieldType mappedFieldType,
        BuilderParams params
    ) {
        super(name, mappedFieldType, params);
        this.indexCreatedVersion = indexCreatedVersion;
        this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
        this.isWithinMultiField = isWithinMultiField;
    }

    /**
     * Returns whether this field mapper needs to support synthetic source.
     */
    protected boolean needsToSupportSyntheticSource() {
        if (multiFieldsNotStoredByDefault_indexVersionCheck()) {
            // if we're within a multi field, then supporting synthetic source isn't necessary as that's the responsibility of the parent
            return isSyntheticSourceEnabled && isWithinMultiField == false;
        }
        return isSyntheticSourceEnabled;
    }

    private boolean multiFieldsNotStoredByDefault_indexVersionCheck() {
        return TextFamilyFieldMapper.multiFieldsNotStoredByDefault_indexVersionCheck(indexCreatedVersion);
    }

    /**
     * Returns whether the current index version supports not storing fields by default when they're multi fields.
     */
    protected static boolean multiFieldsNotStoredByDefault_indexVersionCheck(final IndexVersion indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(IndexVersions.MAPPER_TEXT_MATCH_ONLY_MULTI_FIELDS_DEFAULT_NOT_STORED)
            || indexCreatedVersion.between(
                IndexVersions.MAPPER_TEXT_MATCH_ONLY_MULTI_FIELDS_DEFAULT_NOT_STORED_8_19,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

    /**
     * Returns whether the current index version supports not storing keyword multi fields when they trip ignore_above. The consequence
     * of this check is that the store parameter will be simplified and defaulted to false.
     */
    protected static boolean keywordMultiFieldsNotStoredWhenIgnored_indexVersionCheck(final IndexVersion indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(IndexVersions.KEYWORD_MULTI_FIELDS_NOT_STORED_WHEN_IGNORED);
    }

    public abstract static class Builder extends FieldMapper.Builder {

        public final boolean isSyntheticSourceEnabled;
        public final boolean isWithinMultiField;

        protected Builder(final String name, final boolean isSyntheticSourceEnabled, final boolean isWithinMultiField) {
            super(name);
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.isWithinMultiField = isWithinMultiField;
        }

    }

    public abstract static class TextFamilyFieldType extends StringFieldType {

        protected final boolean isSyntheticSourceEnabled;
        protected final boolean isWithinMultiField;

        public TextFamilyFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            TextSearchInfo textSearchInfo,
            Map<String, String> meta,
            boolean isSyntheticSourceEnabled,
            boolean isWithinMultiField
        ) {
            super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.isWithinMultiField = isWithinMultiField;
        }

        /**
         * Returns the name of the "fallback" field that can be used for synthetic source when the "main" field was not
         * stored for whatever reason.
         */
        public String syntheticSourceFallbackFieldName() {
            return isSyntheticSourceEnabled ? name() + "._original" : null;
        }

    }

}
