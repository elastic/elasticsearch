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
import org.elasticsearch.index.IndexSettings;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains lookup information needed to perform custom synthetic source logic.
 * For example fields that use fallback synthetic source implementation or fields that preserve array ordering
 * in synthetic source;
 */
public class CustomSyntheticSourceFieldLookup {
    private final Map<String, Reason> fieldsWithCustomSyntheticSourceHandling;

    public CustomSyntheticSourceFieldLookup(Mapping mapping, @Nullable IndexSettings indexSettings, boolean isSourceSynthetic) {
        var fields = new HashMap<String, Reason>();
        if (isSourceSynthetic && indexSettings != null) {
            populateFields(fields, mapping.getRoot(), indexSettings.sourceKeepMode());
        }
        this.fieldsWithCustomSyntheticSourceHandling = Map.copyOf(fields);
    }

    private void populateFields(Map<String, Reason> fields, ObjectMapper currentLevel, Mapper.SourceKeepMode defaultSourceKeepMode) {
        if (currentLevel.isEnabled() == false) {
            fields.put(currentLevel.fullPath(), Reason.DISABLED_OBJECT);
            return;
        }
        if (sourceKeepMode(currentLevel, defaultSourceKeepMode) == Mapper.SourceKeepMode.ALL) {
            fields.put(currentLevel.fullPath(), Reason.SOURCE_KEEP_ALL);
            return;
        }
        if (currentLevel.isNested() == false && sourceKeepMode(currentLevel, defaultSourceKeepMode) == Mapper.SourceKeepMode.ARRAYS) {
            fields.put(currentLevel.fullPath(), Reason.SOURCE_KEEP_ARRAYS);
        }

        for (Mapper child : currentLevel) {
            if (child instanceof ObjectMapper objectMapper) {
                populateFields(fields, objectMapper, defaultSourceKeepMode);
            } else if (child instanceof FieldMapper fieldMapper) {
                // The order here is important.
                // If fallback logic is used, it should be always correctly marked as FALLBACK_SYNTHETIC_SOURCE.
                // This allows us to apply an optimization for SOURCE_KEEP_ARRAYS and don't store arrays that have one element.
                // If this order is changed and a field that both has SOURCE_KEEP_ARRAYS and FALLBACK_SYNTHETIC_SOURCE
                // is marked as SOURCE_KEEP_ARRAYS we would lose data for this field by applying such an optimization.
                if (fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK) {
                    fields.put(fieldMapper.fullPath(), Reason.FALLBACK_SYNTHETIC_SOURCE);
                } else if (sourceKeepMode(fieldMapper, defaultSourceKeepMode) == Mapper.SourceKeepMode.ALL) {
                    fields.put(fieldMapper.fullPath(), Reason.SOURCE_KEEP_ALL);
                } else if (sourceKeepMode(fieldMapper, defaultSourceKeepMode) == Mapper.SourceKeepMode.ARRAYS) {
                    fields.put(fieldMapper.fullPath(), Reason.SOURCE_KEEP_ARRAYS);
                }
            }
        }
    }

    private Mapper.SourceKeepMode sourceKeepMode(ObjectMapper mapper, Mapper.SourceKeepMode defaultSourceKeepMode) {
        return mapper.sourceKeepMode().orElse(defaultSourceKeepMode);
    }

    private Mapper.SourceKeepMode sourceKeepMode(FieldMapper mapper, Mapper.SourceKeepMode defaultSourceKeepMode) {
        return mapper.sourceKeepMode().orElse(defaultSourceKeepMode);
    }

    public Map<String, Reason> getFieldsWithCustomSyntheticSourceHandling() {
        return fieldsWithCustomSyntheticSourceHandling;
    }

    /**
     * Specifies why this field needs custom handling.
     */
    public enum Reason {
        SOURCE_KEEP_ARRAYS,
        SOURCE_KEEP_ALL,
        FALLBACK_SYNTHETIC_SOURCE,
        DISABLED_OBJECT
    }
}
