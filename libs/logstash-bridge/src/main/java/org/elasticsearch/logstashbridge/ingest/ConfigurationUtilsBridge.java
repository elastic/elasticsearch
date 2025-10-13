/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.logstashbridge.script.ScriptServiceBridge;
import org.elasticsearch.logstashbridge.script.TemplateScriptFactoryBridge;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Map;

/**
 * An external bridge for {@link ConfigurationUtils}
 */
public class ConfigurationUtilsBridge {
    private ConfigurationUtilsBridge() {}

    public static TemplateScriptFactoryBridge compileTemplate(
        final String processorType,
        final String processorTag,
        final String propertyName,
        final String propertyValue,
        final ScriptServiceBridge bridgedScriptService
    ) {
        ScriptService scriptService = bridgedScriptService.toInternal();
        final TemplateScript.Factory templateScriptFactory = ConfigurationUtils.compileTemplate(
            processorType,
            processorTag,
            propertyName,
            propertyValue,
            scriptService
        );
        return TemplateScriptFactoryBridge.fromInternal(templateScriptFactory);
    }

    public static String readStringProperty(
        final String processorType,
        final String processorTag,
        final Map<String, Object> configuration,
        final String propertyName
    ) {
        return ConfigurationUtils.readStringProperty(processorType, processorTag, configuration, propertyName);
    }

    public static Boolean readBooleanProperty(
        final String processorType,
        final String processorTag,
        final Map<String, Object> configuration,
        final String propertyName,
        final boolean defaultValue
    ) {
        return ConfigurationUtils.readBooleanProperty(processorType, processorTag, configuration, propertyName, defaultValue);
    }
}
