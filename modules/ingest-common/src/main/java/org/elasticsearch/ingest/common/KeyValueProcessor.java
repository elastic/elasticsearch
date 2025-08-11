/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * The KeyValueProcessor parses and extracts messages of the `key=value` variety into fields with values of the keys.
 */
public final class KeyValueProcessor extends AbstractProcessor {

    private static final Logger logger = LogManager.getLogger(KeyValueProcessor.class);

    public static final String TYPE = "kv";

    private static final Pattern STRIP_BRACKETS = Pattern.compile("(^[\\(\\[<\"'])|([\\]\\)>\"']$)");

    private final TemplateScript.Factory field;
    private final String fieldSplit;
    private final String valueSplit;
    private final Set<String> includeKeys;
    private final Set<String> excludeKeys;
    private final TemplateScript.Factory targetField;
    private final boolean ignoreMissing;
    private final Consumer<IngestDocument> execution;

    KeyValueProcessor(
        String tag,
        String description,
        TemplateScript.Factory field,
        String fieldSplit,
        String valueSplit,
        Set<String> includeKeys,
        Set<String> excludeKeys,
        TemplateScript.Factory targetField,
        boolean ignoreMissing,
        String trimKey,
        String trimValue,
        boolean stripBrackets,
        String prefix
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.fieldSplit = fieldSplit;
        this.valueSplit = valueSplit;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.ignoreMissing = ignoreMissing;
        this.execution = buildExecution(
            fieldSplit,
            valueSplit,
            field,
            includeKeys,
            excludeKeys,
            targetField,
            ignoreMissing,
            trimKey,
            trimValue,
            stripBrackets,
            prefix
        );
    }

    private static Consumer<IngestDocument> buildExecution(
        String fieldSplit,
        String valueSplit,
        TemplateScript.Factory field,
        Set<String> includeKeys,
        Set<String> excludeKeys,
        TemplateScript.Factory targetField,
        boolean ignoreMissing,
        String trimKey,
        String trimValue,
        boolean stripBrackets,
        String prefix
    ) {
        final Predicate<String> keyFilter;
        if (includeKeys == null) {
            if (excludeKeys == null) {
                keyFilter = key -> true;
            } else {
                keyFilter = key -> excludeKeys.contains(key) == false;
            }
        } else {
            if (excludeKeys == null) {
                keyFilter = includeKeys::contains;
            } else {
                keyFilter = key -> includeKeys.contains(key) && excludeKeys.contains(key) == false;
            }
        }

        final Function<String, String[]> fieldSplitter = buildSplitter(fieldSplit, true);
        Function<String, String[]> valueSplitter = buildSplitter(valueSplit, false);
        final Function<String, String> keyTrimmer = buildTrimmer(trimKey);
        final Function<String, String> bracketStrip;
        if (stripBrackets) {
            bracketStrip = val -> STRIP_BRACKETS.matcher(val).replaceAll("");
        } else {
            bracketStrip = val -> val;
        }
        final Function<String, String> valueTrimmer = buildTrimmer(trimValue);
        return document -> {
            String target = "";
            if (targetField != null) {
                target = document.renderTemplate(targetField);
            }

            final String fieldPathPrefix;
            String keyPrefix = prefix == null ? "" : prefix;
            if (target.isEmpty()) {
                fieldPathPrefix = keyPrefix;
            } else {
                fieldPathPrefix = target + "." + keyPrefix;
            }
            final Function<String, String> keyPrefixer;
            if (fieldPathPrefix.isEmpty()) {
                keyPrefixer = val -> val;
            } else {
                keyPrefixer = val -> fieldPathPrefix + val;
            }
            String path = document.renderTemplate(field);
            if (path.isEmpty() || document.hasField(path, true) == false) {
                if (ignoreMissing) {
                    return;
                } else {
                    throw new IllegalArgumentException("field [" + path + "] doesn't exist");
                }
            }
            String value = document.getFieldValue(path, String.class, ignoreMissing);
            if (value == null) {
                if (ignoreMissing) {
                    return;
                }
                throw new IllegalArgumentException("field [" + path + "] is null, cannot extract key-value pairs.");
            }
            for (String part : fieldSplitter.apply(value)) {
                String[] kv = valueSplitter.apply(part);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("field [" + path + "] does not contain value_split [" + valueSplit + "]");
                }
                String key = keyTrimmer.apply(kv[0]);
                if (keyFilter.test(key)) {
                    append(document, keyPrefixer.apply(key), valueTrimmer.apply(bracketStrip.apply(kv[1])));
                }
            }
        };
    }

    /**
     * Helper method for buildTrimmer and buildSplitter.
     * <p>
     * If trace logging is enabled, then we should log the stacktrace (and so the message can be slightly simpler).
     * On the other hand if trace logging isn't enabled, then we'll need to log some context on the original issue (but not a stacktrace).
     * <p>
     * Regardless of the logging level, we should throw an exception that has the context in its message, which this method builds.
     */
    private static ElasticsearchException logAndBuildException(String message, Throwable error) {
        String cause = error.getClass().getName();
        if (error.getMessage() != null) {
            cause += ": " + error.getMessage();
        }
        String longMessage = message + ": " + cause;
        if (logger.isTraceEnabled()) {
            logger.trace(message, error);
        } else {
            logger.warn(longMessage);
        }
        return new ElasticsearchException(longMessage);
    }

    private static Function<String, String> buildTrimmer(String trim) {
        if (trim == null) {
            return val -> val;
        } else {
            Pattern pattern = Pattern.compile("(^([" + trim + "]+))|([" + trim + "]+$)");
            return val -> {
                try {
                    return pattern.matcher(val).replaceAll("");
                } catch (Exception | StackOverflowError error) {
                    throw logAndBuildException("Error trimming [" + val + "] using pattern [" + trim + "]", error);
                }
            };
        }
    }

    private static Function<String, String[]> buildSplitter(String split, boolean fields) {
        int limit = fields ? 0 : 2;
        if (split.length() > 2 || split.length() == 2 && split.charAt(0) != '\\') {
            Pattern splitPattern = Pattern.compile(split);
            return val -> {
                try {
                    return splitPattern.split(val, limit);
                } catch (Exception | StackOverflowError error) {
                    throw logAndBuildException("Error splitting [" + val + "] using pattern [" + split + "]", error);
                }
            };
        } else {
            return val -> val.split(split, limit);
        }
    }

    TemplateScript.Factory getField() {
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

    TemplateScript.Factory getTargetField() {
        return targetField;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    private static void append(IngestDocument document, String targetField, String value) {
        if (document.hasField(targetField)) {
            document.appendFieldValue(targetField, value);
        } else {
            document.setFieldValue(targetField, value);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        execution.accept(document);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public KeyValueProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            TemplateScript.Factory fieldTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            TemplateScript.Factory targetFieldTemplate = null;
            if (targetField != null) {
                targetFieldTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "target_field", targetField, scriptService);
            }

            String fieldSplit = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field_split");
            String valueSplit = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "value_split");
            String trimKey = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "trim_key");
            String trimValue = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "trim_value");
            String prefix = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "prefix");
            boolean stripBrackets = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "strip_brackets", false);
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
            return new KeyValueProcessor(
                processorTag,
                description,
                fieldTemplate,
                fieldSplit,
                valueSplit,
                includeKeys,
                excludeKeys,
                targetFieldTemplate,
                ignoreMissing,
                trimKey,
                trimValue,
                stripBrackets,
                prefix
            );
        }
    }
}
