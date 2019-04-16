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
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.PatternFormatter;
import org.apache.logging.log4j.core.pattern.PatternParser;

import java.util.List;

/**
 * Pattern converter to format ...
 */
@Plugin(category = PatternConverter.CATEGORY, name = "prependIfAbsent")
@ConverterKeys({"prependIfAbsent"})
public final class PrependWithFieldNameConverter extends LogEventPatternConverter {

    private String fieldPrefix;
    private List<PatternFormatter> formatters;

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static PrependWithFieldNameConverter newInstance(final Configuration config, final String[] options) {
        final PatternParser parser = PatternLayout.createPatternParser(config);

        final String fieldPrefix = options[0];
        final List<PatternFormatter> formatters = parser.parse(options[1]);

        return new PrependWithFieldNameConverter(fieldPrefix, formatters);
    }

    public PrependWithFieldNameConverter(String fieldPrefix, List<PatternFormatter> formatters) {
        super("prependIfAbsent", "prependIfAbsent");
        this.fieldPrefix = fieldPrefix;
        this.formatters = formatters;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage().getFormat().equals("JSON_FORMATTED")) {
            toAppendTo.append(event.getMessage().getFormattedMessage());
        } else {
            toAppendTo.append("\"").append(fieldPrefix).append("\": \"");
            for (final PatternFormatter formatter : formatters) {
                formatter.format(event, toAppendTo);
            }
            toAppendTo.append("\"");
        }
    }
}
