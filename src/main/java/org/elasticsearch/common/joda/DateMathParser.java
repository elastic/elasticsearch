package org.elasticsearch.common.joda;

import org.elasticsearch.ElasticSearchParseException;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.concurrent.TimeUnit;

/**
 */
public class DateMathParser {

    private final FormatDateTimeFormatter dateTimeFormatter;

    private final TimeUnit timeUnit;

    public DateMathParser(FormatDateTimeFormatter dateTimeFormatter, TimeUnit timeUnit) {
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeUnit = timeUnit;
    }

    public long parse(String text, long now) {
        return parse(text, now, false);
    }

    public long parseRoundCeil(String text, long now) {
        return parse(text, now, true);
    }

    public long parse(String text, long now, boolean roundCeil) {
        long time;
        String mathString;
        if (text.startsWith("now")) {
            time = now;
            mathString = text.substring("now".length());
        } else {
            int index = text.indexOf("||");
            String parseString;
            if (index == -1) {
                parseString = text;
                mathString = ""; // nothing else
            } else {
                parseString = text.substring(0, index);
                mathString = text.substring(index + 2);
            }
            if (roundCeil) {
                time = parseRoundCeilStringValue(parseString);
            } else {
                time = parseStringValue(parseString);
            }
        }

        if (mathString.isEmpty()) {
            return time;
        }

        return parseMath(mathString, time, roundCeil);
    }

    private long parseMath(String mathString, long time, boolean roundUp) throws ElasticSearchParseException {
        MutableDateTime dateTime = new MutableDateTime(time, DateTimeZone.UTC);
        try {
            for (int i = 0; i < mathString.length(); ) {
                char c = mathString.charAt(i++);
                int type;
                if (c == '/') {
                    type = 0;
                } else if (c == '+') {
                    type = 1;
                } else if (c == '-') {
                    type = 2;
                } else {
                    throw new ElasticSearchParseException("operator not supported for date math [" + mathString + "]");
                }

                int num;
                if (!Character.isDigit(mathString.charAt(i))) {
                    num = 1;
                } else {
                    int numFrom = i;
                    while (Character.isDigit(mathString.charAt(i))) {
                        i++;
                    }
                    num = Integer.parseInt(mathString.substring(numFrom, i));
                }
                if (type == 0) {
                    // rounding is only allowed on whole numbers
                    if (num != 1) {
                        throw new ElasticSearchParseException("rounding `/` can only be used on single unit types [" + mathString + "]");
                    }
                }
                char unit = mathString.charAt(i++);
                switch (unit) {
                    case 'y':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.yearOfCentury().roundCeiling();
                            } else {
                                dateTime.yearOfCentury().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addYears(num);
                        } else if (type == 2) {
                            dateTime.addYears(-num);
                        }
                        break;
                    case 'M':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.monthOfYear().roundCeiling();
                            } else {
                                dateTime.monthOfYear().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addMonths(num);
                        } else if (type == 2) {
                            dateTime.addMonths(-num);
                        }
                        break;
                    case 'w':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.weekOfWeekyear().roundCeiling();
                            } else {
                                dateTime.weekOfWeekyear().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addWeeks(num);
                        } else if (type == 2) {
                            dateTime.addWeeks(-num);
                        }
                        break;
                    case 'd':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.dayOfMonth().roundCeiling();
                            } else {
                                dateTime.dayOfMonth().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addDays(num);
                        } else if (type == 2) {
                            dateTime.addDays(-num);
                        }
                        break;
                    case 'h':
                    case 'H':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.hourOfDay().roundCeiling();
                            } else {
                                dateTime.hourOfDay().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addHours(num);
                        } else if (type == 2) {
                            dateTime.addHours(-num);
                        }
                        break;
                    case 'm':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.minuteOfHour().roundCeiling();
                            } else {
                                dateTime.minuteOfHour().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addMinutes(num);
                        } else if (type == 2) {
                            dateTime.addMinutes(-num);
                        }
                        break;
                    case 's':
                        if (type == 0) {
                            if (roundUp) {
                                dateTime.secondOfMinute().roundCeiling();
                            } else {
                                dateTime.secondOfMinute().roundFloor();
                            }
                        } else if (type == 1) {
                            dateTime.addSeconds(num);
                        } else if (type == 2) {
                            dateTime.addSeconds(-num);
                        }
                        break;
                    default:
                        throw new ElasticSearchParseException("unit [" + unit + "] not supported for date math [" + mathString + "]");
                }
            }
        } catch (Exception e) {
            if (e instanceof ElasticSearchParseException) {
                throw (ElasticSearchParseException) e;
            }
            throw new ElasticSearchParseException("failed to parse date math [" + mathString + "]");
        }
        return dateTime.getMillis();
    }

    private long parseStringValue(String value) {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (RuntimeException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new ElasticSearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number", e);
            }
        }
    }

    private long parseRoundCeilStringValue(String value) {
        try {
            // we create a date time for inclusive upper range, we "include" by default the day level data
            // so something like 2011-01-01 will include the full first day of 2011.
            // we also use 1970-01-01 as the base for it so we can handle searches like 10:12:55 (just time)
            // since when we index those, the base is 1970-01-01
            MutableDateTime dateTime = new MutableDateTime(1970, 1, 1, 23, 59, 59, 999, DateTimeZone.UTC);
            int location = dateTimeFormatter.parser().parseInto(dateTime, value, 0);
            // if we parsed all the string value, we are good
            if (location == value.length()) {
                return dateTime.getMillis();
            }
            // if we did not manage to parse, or the year is really high year which is unreasonable
            // see if its a number
            if (location <= 0 || dateTime.getYear() > 5000) {
                try {
                    long time = Long.parseLong(value);
                    return timeUnit.toMillis(time);
                } catch (NumberFormatException e1) {
                    throw new ElasticSearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number");
                }
            }
            return dateTime.getMillis();
        } catch (RuntimeException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new ElasticSearchParseException("failed to parse date field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number", e);
            }
        }
    }
}
