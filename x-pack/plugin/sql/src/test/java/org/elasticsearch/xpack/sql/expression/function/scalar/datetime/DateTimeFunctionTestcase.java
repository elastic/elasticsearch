package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.TimeZone;

public abstract class DateTimeFunctionTestcase<F extends DateTimeFunction> extends ESTestCase {

    static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    final DateTime dateTime(long millisSinceEpoch) {
        return new DateTime(millisSinceEpoch, DateTimeZone.forTimeZone(UTC));
    }

    final DateTime dateTime(final int year, final int month, final int day) {
        return new DateTime(year, month, day, 12, 0);
    }

    final Object process(Object value, String timeZone) {
        return process(value, timeZone(timeZone));
    }

    final Object process(Object value, TimeZone timeZone) {
        return process(value, functionContext(timeZone));
    }

    final Object process(Object value, FunctionContext context) {
        return build(value, context).asProcessorDefinition().asProcessor().process(value);
    }

    final void processAndCheck(final DateTime dateTime, final String timeZone, final Object expected) {
        processAndCheck(dateTime, timeZone(timeZone), expected);
    }

    final void processAndCheck(final DateTime dateTime, final TimeZone timeZone, final Object expected) {
        this.processAndCheck(dateTime, timeZone, this.defaultLocale(), expected);
    }

    final void processAndCheck(final DateTime dateTime,
                               final TimeZone timeZone,
                               final Locale locale,
                               final Object expected) {
        Locale backup = null;
        if(null!=locale) {
            backup = Locale.getDefault();
            Locale.setDefault(locale);
        }
        try {
            assertEquals(dateTime + " " + locale, expected, process(dateTime, timeZone));
        } finally {
            if(null!=backup) {
                Locale.setDefault(backup);
            }
        }
    }

    protected FunctionContext functionContext(final TimeZone timeZone) {
        return new FunctionContext(timeZone, defaultLocale());
    }

    abstract F build(Object value, FunctionContext context);

    abstract Locale defaultLocale();

    private TimeZone timeZone(final String timeZone) {
        return TimeZone.getTimeZone(timeZone);
    }
}
