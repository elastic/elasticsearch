/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;

import java.time.ZoneId;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static java.util.Map.entry;

class JavaDateFormatter implements DateFormatter {
    @SuppressWarnings("unchecked")
    private static <T extends DateTimeParser> T defaultRoundUp(T parser) {
        if (parser instanceof JavaTimeDateTimeParser jtp) {
            return (T) defaultRoundUp(jtp);
        }
        if (parser instanceof Iso8601DateTimeParser iso) {
            return (T) defaultRoundUp(iso);
        }
        throw new IllegalArgumentException("Unknown parser implementation " + parser.getClass());
    }

    /**
     * A default transform that allows to round up fields (used for range searches, optional fields missing)
     * it relies on toString implementation of DateTimeFormatter and ChronoField.
     * For instance for pattern
     * the parser would have a toString()
     * <code>
     *  Value(MonthOfYear,2)'/'Value(DayOfMonth,2)'/'Value(YearOfEra,4,19,EXCEEDS_PAD)'
     * 'Value(ClockHourOfAmPm,2)':'Value(MinuteOfHour,2)' 'Text(AmPmOfDay,SHORT)
     * </code>
     * and ChronoField.CLOCK_HOUR_OF_AMPM would have toString() ClockHourOfAmPm
     * this allows the rounding logic to default CLOCK_HOUR_OF_AMPM field instead of HOUR_OF_DAY
     * without this logic, the rounding would result in a conflict as HOUR_OF_DAY would be missing, but CLOCK_HOUR_OF_AMPM would be provided
     */
    private static JavaTimeDateTimeParser defaultRoundUp(JavaTimeDateTimeParser parser) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        builder.append(parser.formatter());

        String parserAsString = parser.formatter().toString();
        if (parserAsString.contains(ChronoField.DAY_OF_YEAR.toString())) {
            builder.parseDefaulting(ChronoField.DAY_OF_YEAR, 1L);
            // TODO ideally we should make defaulting for weekbased year here too,
            // but this will not work when locale is changed
            // weekbased rounding relies on DateFormatters#localDateFromWeekBasedDate
            // Applying month of year or dayOfMonth when weekbased fields are used will result in a conflict
        } else if (parserAsString.contains(IsoFields.WEEK_BASED_YEAR.toString()) == false) {
            builder.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1L);
            builder.parseDefaulting(ChronoField.DAY_OF_MONTH, 1L);
        }

        if (parserAsString.contains(ChronoField.CLOCK_HOUR_OF_AMPM.toString())) {
            builder.parseDefaulting(ChronoField.CLOCK_HOUR_OF_AMPM, 11L);
            builder.parseDefaulting(ChronoField.AMPM_OF_DAY, 1L);
        } else if (parserAsString.contains(ChronoField.HOUR_OF_AMPM.toString())) {
            builder.parseDefaulting(ChronoField.HOUR_OF_AMPM, 11L);
            builder.parseDefaulting(ChronoField.AMPM_OF_DAY, 1L);
        } else {
            builder.parseDefaulting(ChronoField.HOUR_OF_DAY, 23L);
        }
        builder.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 59L);
        builder.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 59L);
        builder.parseDefaulting(ChronoField.NANO_OF_SECOND, 999_999_999L);

        return new JavaTimeDateTimeParser(builder.toFormatter(parser.getLocale()));
    }

    private static Iso8601DateTimeParser defaultRoundUp(Iso8601DateTimeParser parser) {
        return parser.withDefaults(
            Map.ofEntries(
                entry(ChronoField.MONTH_OF_YEAR, 1),
                entry(ChronoField.DAY_OF_MONTH, 1),
                entry(ChronoField.HOUR_OF_DAY, 23),
                entry(ChronoField.MINUTE_OF_HOUR, 59),
                entry(ChronoField.SECOND_OF_MINUTE, 59),
                entry(ChronoField.NANO_OF_SECOND, 999_999_999)
            )
        );
    }

    private final String format;
    private final DateTimePrinter printer;
    private final DateTimeParser[] parsers;
    final DateTimeParser[] roundupParsers;

    // named formatters use default roundUpParser
    JavaDateFormatter(String format, DateTimePrinter printer, DateTimeParser... parsers) {
        this(format, printer, JavaDateFormatter::defaultRoundUp, parsers);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")    // parsers array is read-only, and not stored in any fields, so this is safe
    <T extends DateTimeParser> JavaDateFormatter(
        String format,
        DateTimePrinter printer,
        UnaryOperator<T> generateRoundUpParser,
        T... parsers
    ) {
        if (format.contains("||")) {
            throw new IllegalArgumentException("This class cannot handle multiple format specifiers");
        }
        if (printer == null) {
            throw new IllegalArgumentException("printer may not be null");
        }
        if (parsers.length == 0) {
            throw new IllegalArgumentException("parsers need to be specified");
        }
        verifyPrinterParsers(printer, parsers);

        this.printer = printer;
        this.format = format;
        this.parsers = Arrays.copyOf(parsers, parsers.length, DateTimeParser[].class);
        this.roundupParsers = mapParsers(generateRoundUpParser, parsers);
    }

    private static void verifyPrinterParsers(DateTimePrinter printer, DateTimeParser[] parsers) {
        ZoneId zoneId = printer.getZone();
        Locale locale = printer.getLocale();
        for (DateTimeParser parser : parsers) {
            if (Objects.equals(parser.getZone(), zoneId) == false) {
                throw new IllegalArgumentException("formatters must have the same time zone");
            }
            if (Objects.equals(parser.getLocale(), locale) == false) {
                throw new IllegalArgumentException("formatters must have the same locale");
            }
        }
    }

    static DateFormatter combined(String input, List<DateFormatter> formatters) {
        assert formatters.isEmpty() == false;

        DateTimePrinter printer = null;
        List<DateTimeParser[]> parsers = new ArrayList<>(formatters.size());
        List<DateTimeParser[]> roundUpParsers = new ArrayList<>(formatters.size());

        for (DateFormatter formatter : formatters) {
            JavaDateFormatter javaDateFormatter = (JavaDateFormatter) formatter;
            if (printer == null) {
                printer = javaDateFormatter.printer;
            }
            parsers.add(javaDateFormatter.parsers);
            roundUpParsers.add(javaDateFormatter.roundupParsers);
        }

        return new JavaDateFormatter(
            input,
            printer,
            roundUpParsers.stream().flatMap(Arrays::stream).toArray(DateTimeParser[]::new),
            parsers.stream().flatMap(Arrays::stream).toArray(DateTimeParser[]::new),
            false
        );
    }

    JavaDateFormatter(String format, DateTimePrinter printer, DateTimeParser[] roundupParsers, DateTimeParser[] parsers) {
        this(
            format,
            printer,
            Arrays.copyOf(roundupParsers, roundupParsers.length, DateTimeParser[].class),
            Arrays.copyOf(parsers, parsers.length, DateTimeParser[].class),
            true
        );
    }

    private JavaDateFormatter(
        String format,
        DateTimePrinter printer,
        DateTimeParser[] roundupParsers,
        DateTimeParser[] parsers,
        boolean doValidate
    ) {
        if (doValidate) {
            if (format.contains("||")) {
                throw new IllegalArgumentException("This class cannot handle multiple format specifiers");
            }
            if (printer == null) {
                throw new IllegalArgumentException("printer may not be null");
            }
            if (parsers.length == 0) {
                throw new IllegalArgumentException("parsers need to be specified");
            }
            verifyPrinterParsers(printer, parsers);
        }
        this.format = format;
        this.printer = printer;
        this.roundupParsers = roundupParsers;
        this.parsers = parsers;
    }

    TemporalAccessor roundupParse(String input) {
        if (Strings.isNullOrEmpty(input)) {
            throw new IllegalArgumentException("cannot parse empty datetime");
        }

        try {
            return doParse(input, roundupParsers);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse date field [" + input + "] with format [" + format + "]", e);
        }
    }

    @Override
    public TemporalAccessor parse(String input) {
        if (Strings.isNullOrEmpty(input)) {
            throw new IllegalArgumentException("cannot parse empty datetime");
        }

        try {
            return doParse(input, parsers);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse date field [" + input + "] with format [" + format + "]", e);
        }
    }

    /**
     * Attempt parsing the input without throwing exception. If multiple parsers are provided,
     * it will continue iterating until it finds one that works.
     *
     * @param input An arbitrary string resembling the string representation of a date or time
     * @return a TemporalAccessor if parsing was successful.
     * @throws DateTimeParseException when unable to parse with any parsers
     */
    private static TemporalAccessor doParse(String input, DateTimeParser[] parsers) {
        if (parsers.length > 1) {
            int earliestError = Integer.MAX_VALUE;
            for (DateTimeParser parser : parsers) {
                ParseResult result = parser.tryParse(input);
                if (result.result() != null) {
                    return result.result();
                }
                earliestError = Math.min(earliestError, result.errorIndex());
            }
            throw new DateTimeParseException("Failed to parse with all enclosed parsers", input, earliestError);
        }
        return parsers[0].parse(input);
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcut to not create new objects unnecessarily
        if (zoneId.equals(zone())) {
            return this;
        }
        return mapParsers(p -> p.withZone(zoneId), p -> p.withZone(zoneId));
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcut to not create new objects unnecessarily
        if (locale.equals(locale())) {
            return this;
        }
        return mapParsers(p -> p.withLocale(locale), p -> p.withLocale(locale));
    }

    private JavaDateFormatter mapParsers(UnaryOperator<DateTimePrinter> printerMapping, UnaryOperator<DateTimeParser> parserMapping) {
        return new JavaDateFormatter(
            format,
            printerMapping.apply(printer),
            mapParsers(parserMapping, this.roundupParsers),
            mapParsers(parserMapping, this.parsers),
            false
        );
    }

    @SafeVarargs
    private static <T extends DateTimeParser> DateTimeParser[] mapParsers(UnaryOperator<T> mapping, T... objects) {
        DateTimeParser[] res = new DateTimeParser[objects.length];
        for (int i = 0; i < objects.length; i++) {
            res[i] = mapping.apply(objects[i]);
        }
        return res;
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return printer.format(DateFormatters.from(accessor));
    }

    @Override
    public String pattern() {
        return format;
    }

    @Override
    public Locale locale() {
        return this.printer.getLocale();
    }

    @Override
    public ZoneId zone() {
        return this.printer.getZone();
    }

    @Override
    public DateMathParser toDateMathParser() {
        return new JavaDateMathParser(format, this::parse, this::roundupParse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale(), printer.getZone(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        JavaDateFormatter other = (JavaDateFormatter) obj;

        return Objects.equals(format, other.format)
            && Objects.equals(locale(), other.locale())
            && Objects.equals(this.printer.getZone(), other.printer.getZone());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "format[%s] locale[%s]", format, locale());
    }

}
