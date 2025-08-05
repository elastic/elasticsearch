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
        if (multiFieldsNotStoredByDefaultIndexVersionCheck()) {
            // if we're within a multi field, then supporting synthetic source isn't necessary as that's the
            // responsibility of the parent field
            return isSyntheticSourceEnabled && isWithinMultiField == false;
        }
        return isSyntheticSourceEnabled;
    }

    private boolean multiFieldsNotStoredByDefaultIndexVersionCheck() {
        return indexCreatedVersion.onOrAfter(IndexVersions.MAPPER_TEXT_MATCH_ONLY_MULTI_FIELDS_DEFAULT_NOT_STORED)
            || indexCreatedVersion.between(
                IndexVersions.MAPPER_TEXT_MATCH_ONLY_MULTI_FIELDS_DEFAULT_NOT_STORED_8_19,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

}
