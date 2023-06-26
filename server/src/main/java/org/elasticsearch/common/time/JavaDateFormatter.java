/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;

import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

class JavaDateFormatter implements DateFormatter {
    /**
    * A default consumer that allows to round up fields (used for range searches, optional fields missing)
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
    private static final BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> DEFAULT_ROUND_UP = (builder, parser) -> {
        String parserAsString = parser.toString();
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
    };

    private final String format;
    private final DateTimeFormatter printer;
    private final DateTimeFormatter[] parsers;
    private final RoundUpFormatter roundupParser;

    private static final class RoundUpFormatter extends JavaDateFormatter {

        RoundUpFormatter(String format, DateTimeFormatter[] roundUpParsers) {
            super(format, roundUpParsers[0], (RoundUpFormatter) null, roundUpParsers);
        }

        @Override
        JavaDateFormatter getRoundupParser() {
            throw new UnsupportedOperationException("RoundUpFormatter does not have another roundUpFormatter");
        }
    }

    // named formatters use default roundUpParser
    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        this(
            format,
            printer,
            // set up base fields which should be used for default parsing, when we round up for date math
            DEFAULT_ROUND_UP,
            parsers
        );
    }

    // subclasses override roundUpParser
    JavaDateFormatter(
        String format,
        DateTimeFormatter printer,
        BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> roundupParserConsumer,
        DateTimeFormatter... parsers
    ) {
        if (printer == null) {
            throw new IllegalArgumentException("printer may not be null");
        }
        this.printer = printer;
        this.format = format;
        this.parsers = parsersArray(printer, parsers);
        this.roundupParser = createRoundUpParser(format, roundupParserConsumer, locale(), this.parsers);
    }

    private static DateTimeFormatter[] parsersArray(DateTimeFormatter printer, DateTimeFormatter... parsers) {
        if (parsers.length == 0) {
            return new DateTimeFormatter[] { printer };
        }
        final ZoneId zoneId = parsers[0].getZone();
        final Locale locale = parsers[0].getLocale();
        for (int i = 1; i < parsers.length; i++) {
            final DateTimeFormatter parser = parsers[i];
            if (Objects.equals(parser.getZone(), zoneId) == false) {
                throw new IllegalArgumentException("formatters must have the same time zone");
            }
            if (Objects.equals(parser.getLocale(), locale) == false) {
                throw new IllegalArgumentException("formatters must have the same locale");
            }
        }
        return parsers;
    }

    /**
     * This is when the RoundUp Formatters are created. In further merges (with ||) it will only append them to a list.
     * || is not expected to be provided as format when a RoundUp formatter is created. It will be splitted before in
     * <code>DateFormatter.forPattern</code>
     * JavaDateFormatter created with a custom format like <code>DateFormatter.forPattern("YYYY")</code> will only have one parser
     * It is however possible to have a JavaDateFormatter with multiple parsers. For instance see a "date_time" formatter in
     * <code>DateFormatters</code>.
     * This means that we need to also have multiple RoundUp parsers.
     */
    private static RoundUpFormatter createRoundUpParser(
        String format,
        BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> roundupParserConsumer,
        Locale locale,
        DateTimeFormatter[] parsers
    ) {
        if (format.contains("||") == false) {
            return new RoundUpFormatter(format, mapParsers(parser -> {
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                builder.append(parser);
                roundupParserConsumer.accept(builder, parser);
                return builder.toFormatter(locale);
            }, parsers));
        }
        return null;
    }

    public static DateFormatter combined(String input, List<DateFormatter> formatters) {
        assert formatters.size() > 0;

        List<DateTimeFormatter> parsers = new ArrayList<>(formatters.size());
        List<DateTimeFormatter> roundUpParsers = new ArrayList<>(formatters.size());

        DateTimeFormatter printer = null;
        for (DateFormatter formatter : formatters) {
            assert formatter instanceof JavaDateFormatter;
            JavaDateFormatter javaDateFormatter = (JavaDateFormatter) formatter;
            if (printer == null) {
                printer = javaDateFormatter.getPrinter();
            }
            Collections.addAll(parsers, javaDateFormatter.parsers);
            Collections.addAll(roundUpParsers, javaDateFormatter.getRoundupParser().parsers);
        }

        return new JavaDateFormatter(
            input,
            printer,
            roundUpParsers.toArray(new DateTimeFormatter[0]),
            parsers.toArray(new DateTimeFormatter[0])
        );
    }

    private JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter[] roundUpParsers, DateTimeFormatter[] parsers) {
        this(format, printer, new RoundUpFormatter(format, roundUpParsers), parsers);
    }

    private JavaDateFormatter(String format, DateTimeFormatter printer, RoundUpFormatter roundupParser, DateTimeFormatter[] parsers) {
        this.format = format;
        this.printer = printer;
        this.roundupParser = roundupParser;
        this.parsers = parsers;
    }

    JavaDateFormatter getRoundupParser() {
        return roundupParser;
    }

    DateTimeFormatter getPrinter() {
        return printer;
    }

    @Override
    public TemporalAccessor parse(String input) {
        if (Strings.isNullOrEmpty(input)) {
            throw new IllegalArgumentException("cannot parse empty date");
        }

        try {
            return doParse(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse date field [" + input + "] with format [" + format + "]", e);
        }
    }

    /**
     * Attempt parsing the input without throwing exception. If multiple parsers are provided,
     * it will continue iterating if the previous parser failed. The pattern must fully match, meaning whole input was used.
     * This also means that this method depends on <code>DateTimeFormatter.ClassicFormat.parseObject</code>
     * which does not throw exceptions when parsing failed.
     *
     * The approach with collection of parsers was taken because java-time requires ordering on optional (composite)
     * patterns. Joda does not suffer from this.
     * https://bugs.openjdk.java.net/browse/JDK-8188771
     *
     * @param input An arbitrary string resembling the string representation of a date or time
     * @return a TemporalAccessor if parsing was successful.
     * @throws DateTimeParseException when unable to parse with any parsers
     */
    private TemporalAccessor doParse(String input) {
        if (parsers.length > 1) {
            for (DateTimeFormatter formatter : parsers) {
                ParsePosition pos = new ParsePosition(0);
                Object object = formatter.toFormat().parseObject(input, pos);
                if (parsingSucceeded(object, input, pos)) {
                    return (TemporalAccessor) object;
                }
            }
            throw new DateTimeParseException("Failed to parse with all enclosed parsers", input, 0);
        }
        return this.parsers[0].parse(input);
    }

    private static boolean parsingSucceeded(Object object, String input, ParsePosition pos) {
        return object != null && pos.getIndex() == input.length();
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(zone())) {
            return this;
        }
        return mapParsers(p -> p.withZone(zoneId));
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcurt to not create new objects unnecessarily
        if (locale.equals(locale())) {
            return this;
        }
        return mapParsers(p -> p.withLocale(locale));
    }

    private JavaDateFormatter mapParsers(UnaryOperator<DateTimeFormatter> mapping) {
        return new JavaDateFormatter(
            format,
            mapping.apply(printer),
            mapParsers(mapping, ((JavaDateFormatter) this.roundupParser).parsers),
            mapParsers(mapping, this.parsers)
        );
    }

    private static DateTimeFormatter[] mapParsers(UnaryOperator<DateTimeFormatter> mapping, DateTimeFormatter[] parsers) {
        DateTimeFormatter[] res = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            res[i] = mapping.apply(parsers[i]);
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
        return new JavaDateMathParser(format, this, getRoundupParser());
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
