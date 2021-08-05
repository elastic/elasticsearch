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
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class JavaDateFormatter implements DateFormatter {

    // base fields which should be used for default parsing, when we round up for date math
    private static final Map<TemporalField, Long> ROUND_UP_BASE_FIELDS = new HashMap<>(6);

    {
        ROUND_UP_BASE_FIELDS.put(ChronoField.MONTH_OF_YEAR, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.DAY_OF_MONTH, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.HOUR_OF_DAY, 23L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MINUTE_OF_HOUR, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.SECOND_OF_MINUTE, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.NANO_OF_SECOND, 999_999_999L);
    }

    private final String format;
    private final DateTimeFormatter printer;
    private final List<DateTimeFormatter> parsers;
    private final JavaDateFormatter roundupParser;

    static class RoundUpFormatter extends JavaDateFormatter{

        RoundUpFormatter(String format, List<DateTimeFormatter> roundUpParsers) {
            super(format,  firstFrom(roundUpParsers),null, roundUpParsers);
        }

        private static DateTimeFormatter firstFrom(List<DateTimeFormatter> roundUpParsers) {
            return roundUpParsers.get(0);
        }

        @Override
        JavaDateFormatter getRoundupParser() {
            throw new UnsupportedOperationException("RoundUpFormatter does not have another roundUpFormatter");
        }
    }

    // named formatters use default roundUpParser
    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        this(format, printer, builder -> ROUND_UP_BASE_FIELDS.forEach(builder::parseDefaulting), parsers);
    }

    // subclasses override roundUpParser
    JavaDateFormatter(String format,
                      DateTimeFormatter printer,
                      Consumer<DateTimeFormatterBuilder> roundupParserConsumer,
                      DateTimeFormatter... parsers) {
        if (printer == null) {
            throw new IllegalArgumentException("printer may not be null");
        }
        long distinctZones = Arrays.stream(parsers).map(DateTimeFormatter::getZone).distinct().count();
        if (distinctZones > 1) {
            throw new IllegalArgumentException("formatters must have the same time zone");
        }
        long distinctLocales = Arrays.stream(parsers).map(DateTimeFormatter::getLocale).distinct().count();
        if (distinctLocales > 1) {
            throw new IllegalArgumentException("formatters must have the same locale");
        }
        this.printer = printer;
        this.format = format;

        if (parsers.length == 0) {
            this.parsers = Collections.singletonList(printer);
        } else {
            this.parsers = Arrays.asList(parsers);
        }
        List<DateTimeFormatter> roundUp = createRoundUpParser(format, roundupParserConsumer);
        this.roundupParser = new RoundUpFormatter(format, roundUp) ;
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
    private List<DateTimeFormatter> createRoundUpParser(String format,
                                                        Consumer<DateTimeFormatterBuilder> roundupParserConsumer) {
        if (format.contains("||") == false) {
            List<DateTimeFormatter> roundUpParsers = new ArrayList<>();
            for (DateTimeFormatter parser : this.parsers) {
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                builder.append(parser);
                roundupParserConsumer.accept(builder);
                roundUpParsers.add(builder.toFormatter(locale()));
            }
            return roundUpParsers;
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
            parsers.addAll(javaDateFormatter.getParsers());
            roundUpParsers.addAll(javaDateFormatter.getRoundupParser().getParsers());
        }

        return new JavaDateFormatter(input, printer, roundUpParsers, parsers);
    }

     private JavaDateFormatter(String format, DateTimeFormatter printer, List<DateTimeFormatter> roundUpParsers,
                               List<DateTimeFormatter> parsers) {
        this.format = format;
        this.printer = printer;
        this.roundupParser = roundUpParsers != null ? new RoundUpFormatter(format,  roundUpParsers ) : null;
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
        if (parsers.size() > 1) {
            for (DateTimeFormatter formatter : parsers) {
                ParsePosition pos = new ParsePosition(0);
                Object object = formatter.toFormat().parseObject(input, pos);
                if (parsingSucceeded(object, input, pos)) {
                    return (TemporalAccessor) object;
                }
            }
            throw new DateTimeParseException("Failed to parse with all enclosed parsers", input, 0);
        }
        return this.parsers.get(0).parse(input);
    }

    private boolean parsingSucceeded(Object object, String input, ParsePosition pos) {
        return object != null && pos.getIndex() == input.length();
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(zone())) {
            return this;
        }
        List<DateTimeFormatter> parsers = this.parsers.stream().map(p -> p.withZone(zoneId)).collect(Collectors.toList());
        List<DateTimeFormatter> roundUpParsers = this.roundupParser.getParsers()
                                                                   .stream()
                                                                   .map(p -> p.withZone(zoneId))
                                                                   .collect(Collectors.toList());
        return new JavaDateFormatter(format, printer.withZone(zoneId), roundUpParsers, parsers);
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcurt to not create new objects unnecessarily
        if (locale.equals(locale())) {
            return this;
        }
        List<DateTimeFormatter> parsers = this.parsers.stream().map(p -> p.withLocale(locale)).collect(Collectors.toList());
        List<DateTimeFormatter> roundUpParsers = this.roundupParser.getParsers()
                                                                   .stream()
                                                                   .map(p -> p.withLocale(locale))
                                                                   .collect(Collectors.toList());
        return new JavaDateFormatter(format, printer.withLocale(locale), roundUpParsers, parsers);
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

        return Objects.equals(format, other.format) &&
            Objects.equals(locale(), other.locale()) &&
            Objects.equals(this.printer.getZone(), other.printer.getZone());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "format[%s] locale[%s]", format, locale());
    }

    Collection<DateTimeFormatter> getParsers() {
        return parsers;
    }
}
