/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.Map;

/**
 * This is a quality of life class that adds synthetic source context for text fields that need it.
 */
public abstract class TextFamilyFieldType extends StringFieldType {

    private final boolean isSyntheticSourceEnabled;
    private final boolean isWithinMultiField;

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

    public boolean isSyntheticSourceEnabled() {
        return isSyntheticSourceEnabled;
    }

    public boolean isWithinMultiField() {
        return isWithinMultiField;
    }

    /**
     * Returns the name of the "fallback" field that can be used for synthetic source when the "main" field was not
     * stored for whatever reason.
     */
    public String syntheticSourceFallbackFieldName() {
        return isSyntheticSourceEnabled ? name() + "._original" : null;
    }

}
