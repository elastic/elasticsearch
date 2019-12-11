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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import java.util.Set;

/**
 * Pattern converter to populate CustomMapFields in a pattern.
 * This is to be used with custom ElasticSearch log messages
 * It will only populate these if the event have message of type <code>ESLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "CustomMapFields")
@ConverterKeys({"CustomMapFields"})
public final class CustomMapFieldsConverter extends LogEventPatternConverter {


    private Set<String> overridenFields;

    public CustomMapFieldsConverter(Set<String> overridenFields) {
        super("CustomMapFields", "CustomMapFields");
        this.overridenFields = overridenFields;
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static CustomMapFieldsConverter newInstance(final Configuration config, final String[] options) {
        Set<String> overridenFields = csvToSet(options[0]);
        return new CustomMapFieldsConverter(overridenFields);
    }

    private static Set<String> csvToSet(String csv) {
        String[] split = csv.split(",");
        return Set.of(split);
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if(event.getMessage() instanceof ESLogMessage) {
            ESLogMessage logMessage = (ESLogMessage) event.getMessage();
            logMessage.asJson(toAppendTo);
        }
    }
}
