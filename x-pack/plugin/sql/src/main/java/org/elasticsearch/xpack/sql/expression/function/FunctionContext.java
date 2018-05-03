/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LocaleUtils;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Rather than pass around numerous defaults for functions, all default type values are passed in
 * via a function context.
 */
public final class FunctionContext {

    public static FunctionContext read(final StreamInput in) throws IOException {
        final TimeZone timeZone = TimeZone.getTimeZone(in.readString());
        final Locale locale = LocaleUtils.parse(in.readString());
        return new FunctionContext(timeZone, locale);
    }

    /**
     * Time zone in which we're executing this SQL. It is attached to functions
     * that deal with date and time.
     */
    private final TimeZone timeZone;
    /**
     * Locale in which we're executing this SQL. It is attached to functions
     * that deal with date and time.
     */
    private final Locale locale;

    public FunctionContext(final TimeZone timeZone, final Locale locale) {
        Objects.requireNonNull(timeZone, "timeZone");
        Objects.requireNonNull(locale, "locale");

        this.timeZone = timeZone;
        this.locale = locale;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    public Locale locale() {
        return this.locale;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(timeZone().getID());
        out.writeString(locale().toString());
    }

    public int hashCode() {
        return Objects.hash(this.timeZone(), this.locale());
    }

    public boolean equals(final Object other) {
        return this == other || other instanceof FunctionContext && equals0((FunctionContext) other);
    }

    private boolean equals0(final FunctionContext other) {
        return this.timeZone().equals(other.timeZone()) &&
            this.locale().equals(other.locale());
    }

    public String toString() {
        return this.timeZone().getID() + " " + this.locale();
    }
}
