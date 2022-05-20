/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.ExtendedThrowablePatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.nio.charset.Charset;
import java.util.StringJoiner;

/**

 * Outputs the Throwable portion of the LoggingEvent as a Json formatted field with array
 * "exception": [ "stacktrace", "lines", "as", "array", "elements" ]
 *
 * Reusing @link org.apache.logging.log4j.core.pattern.ExtendedThrowablePatternConverter which already converts a Throwable from
 * LoggingEvent into a multiline string
 */
@Plugin(name = "JsonThrowablePatternConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({ "exceptionAsJson" })
public final class JsonThrowablePatternConverter extends ThrowablePatternConverter {
    private final ExtendedThrowablePatternConverter throwablePatternConverter;

    /**
     * Private as only expected to be used by log4j2 newInstance method
     */
    private JsonThrowablePatternConverter(final Configuration config, final String[] options) {
        super("JsonThrowablePatternConverter", "throwable", options, config);
        this.throwablePatternConverter = ExtendedThrowablePatternConverter.newInstance(config, options);
    }

    /**
     * Gets an instance of the class.
     *
     * @param config  The current Configuration.
     * @param options pattern options, may be null.  If first element is "short",
     *                only the first line of the throwable will be formatted.
     * @return instance of class.
     */
    public static JsonThrowablePatternConverter newInstance(final Configuration config, final String[] options) {
        return new JsonThrowablePatternConverter(config, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        String consoleStacktrace = formatStacktrace(event);
        if (Strings.isNotEmpty(consoleStacktrace)) {
            String jsonStacktrace = formatJson(consoleStacktrace);

            toAppendTo.append(", ");
            toAppendTo.append(jsonStacktrace);
        }
    }

    private String formatStacktrace(LogEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        throwablePatternConverter.format(event, stringBuilder);
        return stringBuilder.toString();
    }

    private String formatJson(String consoleStacktrace) {
        String lineSeparator = options.getSeparator() + "\t|" + options.getSeparator();
        String[] split = consoleStacktrace.split(lineSeparator);

        StringJoiner stringJoiner = new StringJoiner(",\n", "\n\"stacktrace\": [", "]");
        for (String line : split) {
            stringJoiner.add(wrapAsJson(line));
        }
        return stringJoiner.toString();
    }

    private static String wrapAsJson(String line) {
        byte[] bytes = JsonStringEncoder.getInstance().quoteAsUTF8(line);
        return "\"" + new String(bytes, Charset.defaultCharset()) + "\"";
    }

    @Override
    public boolean handlesThrowable() {
        return true;
    }
}
