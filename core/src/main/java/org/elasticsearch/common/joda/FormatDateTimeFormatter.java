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

package org.elasticsearch.common.joda;

import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Objects;

/**
 * A simple wrapper around {@link DateTimeFormatter} that retains the
 * format that was used to create it.
 */
public class FormatDateTimeFormatter {

    private final String format;

    private final DateTimeFormatter parser;

    private final DateTimeFormatter printer;

    private final Locale locale;

    public FormatDateTimeFormatter(String format, DateTimeFormatter parser, Locale locale) {
        this(format, parser, parser, locale);
    }

    public FormatDateTimeFormatter(String format, DateTimeFormatter parser, DateTimeFormatter printer, Locale locale) {
        this.format = format;
        this.locale = Objects.requireNonNull(locale, "A locale is required as JODA otherwise uses the default locale");
        this.printer = printer.withLocale(locale).withDefaultYear(1970);
        this.parser = parser.withLocale(locale).withDefaultYear(1970);
    }

    public String format() {
        return format;
    }

    public DateTimeFormatter parser() {
        return parser;
    }

    public DateTimeFormatter printer() {
        return this.printer;
    }

    public Locale locale() {
        return locale;
    }
}
