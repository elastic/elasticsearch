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

import java.util.HashMap;
import java.util.Map;

/**
 * Contains lookup information needed to perform custom synthetic source logic.
 * For example fields that use fallback synthetic source implementation or fields that preserve array ordering
 * in synthetic source;
 */
public class CustomSyntheticSourceFieldLookup {
    private final Map<String, Reason> fieldsWithCustomSyntheticSourceHandling;

    public CustomSyntheticSourceFieldLookup(Mapping mapping, IndexSettings indexSettings, boolean isSourceSynthetic) {
        this.fieldsWithCustomSyntheticSourceHandling = new HashMap<>();
        if (isSourceSynthetic) {
            populateFields(fieldsWithCustomSyntheticSourceHandling, mapping.getRoot(), indexSettings.sourceKeepMode());
        }
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
