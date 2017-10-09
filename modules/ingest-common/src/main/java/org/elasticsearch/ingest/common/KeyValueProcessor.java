/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The KeyValueProcessor parses and extracts messages of the `key=value` variety into fields with values of the keys.
 */
public final class KeyValueProcessor extends AbstractProcessor {

    public static final String TYPE = "kv";

    private final String field;
    private final String fieldSplit;
    private final String valueSplit;
    private final Set<String> includeKeys;
    private final Set<String> excludeKeys;
    private final String targetField;
    private final boolean ignoreMissing;

    KeyValueProcessor(String tag, String field, String fieldSplit, String valueSplit, Set<String> includeKeys,
                      Set<String> excludeKeys, String targetField, boolean ignoreMissing) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.fieldSplit = fieldSplit;
        this.valueSplit = valueSplit;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.ignoreMissing = ignoreMissing;
    }

    String getField() {
        return field;
    }

    String getFieldSplit() {
        return fieldSplit;
    }

    String getValueSplit() {
        return valueSplit;
    }

    Set<String> getIncludeKeys() {
        return includeKeys;
    }

    Set<String> getExcludeKeys() {
        return excludeKeys;
    }

    String getTargetField() {
        return targetField;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    public void append(IngestDocument document, String targetField, String value) {
        if (document.hasField(targetField)) {
            document.appendFieldValue(targetField, value);
        } else {
            document.setFieldValue(targetField, value);
        }
    }

    @Override
    public void execute(IngestDocument document) {
        String oldVal = document.getFieldValue(field, String.class, ignoreMissing);

        if (oldVal == null && ignoreMissing) {
            return;
        } else if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract key-value pairs.");
        }

        String fieldPathPrefix = (targetField == null) ? "" : targetField + ".";
        Arrays.stream(oldVal.split(fieldSplit))
            .map((f) -> {
                String[] kv = f.split(valueSplit, 2);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("field [" + field + "] does not contain value_split [" + valueSplit + "]");
                }
                return kv;
            })
            .filter((p) ->
                (includeKeys == null || includeKeys.contains(p[0])) &&
                    (excludeKeys == null || excludeKeys.contains(p[0]) == false))
            .forEach((p) -> append(document, fieldPathPrefix + p[0], p[1]));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {
        @Override
        public KeyValueProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                        Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            String fieldSplit = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field_split");
            String valueSplit = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "value_split");
            Set<String> includeKeys = null;
            Set<String> excludeKeys = null;
            List<String> includeKeysList = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "include_keys");
            if (includeKeysList != null) {
                includeKeys = Collections.unmodifiableSet(Sets.newHashSet(includeKeysList));
            }
            List<String> excludeKeysList = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "exclude_keys");
            if (excludeKeysList != null) {
                excludeKeys = Collections.unmodifiableSet(Sets.newHashSet(excludeKeysList));
            }
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new KeyValueProcessor(processorTag, field, fieldSplit, valueSplit, includeKeys, excludeKeys, targetField, ignoreMissing);
        }
    }
}
