/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 *
 */
public class DynamicIndexName implements ToXContent {

    public static final String DEFAULT_DATE_FORMAT = "YYYY.MM.dd";

    private static final String EXPRESSION_LEFT_BOUND = "<";
    private static final String EXPRESSION_RIGHT_BOUND = ">";
    private static final char LEFT_BOUND = '{';
    private static final char RIGHT_BOUND = '}';
    private static final char ESCAPE_CHAR = '\\';

    private final String text;
    private final Expression expression;

    private DynamicIndexName(String text, Expression expression) {
        this.text = text;
        this.expression = expression;
    }

    public String text() {
        return text;
    }

    public String name(DateTime now) {
        return expression.eval(now);
    }

    public static String[] names(DynamicIndexName[] indexNames, DateTime now) {
        String[] names = new String[indexNames.length];
        for (int i = 0; i < names.length; i++) {
            names[i] = indexNames[i].name(now);
        }
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DynamicIndexName that = (DynamicIndexName) o;

        return text.equals(that.text);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(text);
    }

    public static String defaultDateFormat(Settings settings, String componentPrefix) {
        if (componentPrefix == null) {
            return defaultDateFormat(settings);
        }
        return settings.get(componentPrefix + ".dynamic_indices.default_date_format", defaultDateFormat(settings));
    }

    public static String defaultDateFormat(Settings settings) {
        return settings.get("watcher.dynamic_indices.default_date_format", DEFAULT_DATE_FORMAT);
    }

    public static DateTimeZone timeZone(Settings settings, String componentPrefix) {
        if (componentPrefix == null) {
            return timeZone(settings);
        }

        String timeZoneId = settings.get(componentPrefix + ".dynamic_indices.time_zone", DateTimeZone.UTC.getID());
        return DateTimeZone.forID(timeZoneId);
    }

    public static DateTimeZone timeZone(Settings settings) {
        String timeZoneId = settings.get("watcher.dynamic_indices.time_zone", DateTimeZone.UTC.getID());
        return DateTimeZone.forID(timeZoneId);
    }

    interface Expression {

        String eval(DateTime now);

    }

    static class StaticExpression implements Expression {

        private final String value;

        public StaticExpression(String value) {
            this.value = value;
        }

        @Override
        public String eval(DateTime now) {
            return value;
        }
    }

    static class DateMathExpression implements Expression {

        private final DateMathParser dateMathParser;
        private final String mathExpression;
        private final FormatDateTimeFormatter formatter;
        private final DateTimeZone timeZone;

        public DateMathExpression(String defaultFormat, DateTimeZone timeZone, String expression) {
            this.timeZone = timeZone;
            int i = expression.indexOf(LEFT_BOUND);
            String format;
            if (i < 0) {
                mathExpression = expression;
                format = defaultFormat;
            } else {
                if (expression.lastIndexOf(RIGHT_BOUND) != expression.length() - 1) {
                    throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing closing `}` for date math format", expression);
                }
                if (i == expression.length() - 2) {
                    throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing date format", expression);
                }
                mathExpression = expression.substring(0, i);
                format = expression.substring(i + 1, expression.length() - 1);

            }
            DateTimeFormatter parser = DateTimeFormat.forPattern(format).withZone(timeZone);
            formatter = new FormatDateTimeFormatter(defaultFormat, parser, Locale.ROOT);
            dateMathParser = new DateMathParser(formatter);
        }

        @Override
        public String eval(final DateTime now) {
            long millis = dateMathParser.parse(mathExpression, new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return now.getMillis();
                }
            }, false, timeZone);
            return formatter.printer().print(millis);
        }
    }

    static class CompoundExpression implements Expression {

        private final Expression[] parts;

        public CompoundExpression(Expression[] parts) {
            this.parts = parts;
        }

        @Override
        public String eval(DateTime now) {
            StringBuilder sb = new StringBuilder();
            for (Expression part : parts) {
                sb.append(part.eval(now));
            }
            return sb.toString();
        }

        static Expression parse(String defaultDateFormat, DateTimeZone timeZone, char[] text, int from, int length) {
            boolean dynamic = false;
            List<Expression> expressions = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            boolean inPlaceHolder = false;
            boolean inDateFormat = false;
            boolean escape = false;
            for (int i = from; i < length; i++) {
                boolean escapedChar = escape;
                if (escape) {
                    escape = false;
                }
                char c = text[i];

                if (c == ESCAPE_CHAR) {
                    if (escapedChar) {
                        sb.append(c);
                        escape = false;
                    } else {
                        escape = true;
                    }
                    continue;
                }

                if (inPlaceHolder) {
                    switch (c) {

                        case LEFT_BOUND:
                            if (inDateFormat && escapedChar) {
                                sb.append(c);
                            } else if (!inDateFormat) {
                                inDateFormat = true;
                                sb.append(c);
                            } else {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. invalid character in placeholder at position [{}]", new String(text, from, length), i);
                            }
                            break;

                        case RIGHT_BOUND:
                            if (inDateFormat && escapedChar) {
                                sb.append(c);
                            } else if (inDateFormat) {
                                inDateFormat = false;
                                sb.append(c);
                            } else {
                                expressions.add(new DateMathExpression(defaultDateFormat, timeZone, sb.toString()));
                                sb = new StringBuilder();
                                inPlaceHolder = false;
                                dynamic = true;
                            }
                            break;

                        default:
                            sb.append(c);
                    }
                } else {
                    switch (c) {

                        case LEFT_BOUND:
                            if (escapedChar) {
                                sb.append(c);
                            } else {
                                expressions.add(new StaticExpression(sb.toString()));
                                sb = new StringBuilder();
                                inPlaceHolder = true;
                            }
                            break;

                        case RIGHT_BOUND:
                            if (!escapedChar) {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. invalid character at position [{}]. " +
                                        "`{` and `}` are reserved characters and should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)", new String(text, from, length), i);
                            }
                        default:
                            sb.append(c);
                    }
                }
            }
            if (inPlaceHolder) {
                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. date math placeholder is open ended", new String(text, from, length));
            }
            if (sb.length() > 0) {
                expressions.add(new StaticExpression(sb.toString()));
            }

            if (!dynamic) {
                // if all the expressions are static... lets optimize to a single static expression
                sb = new StringBuilder();
                for (Expression expression : expressions) {
                    sb.append(((StaticExpression) expression).value);
                }
                return new StaticExpression(sb.toString());
            }

            if (expressions.size() == 1) {
                return expressions.get(0);
            }

            return new CompoundExpression(expressions.toArray(new Expression[expressions.size()]));
        }
    }

    public static class Parser {

        private final String defaultDateFormat;
        private final DateTimeZone timeZone;

        public Parser() {
            this(DEFAULT_DATE_FORMAT, DateTimeZone.UTC);
        }

        public Parser(String defaultDateFormat, DateTimeZone timeZone) {
            this.defaultDateFormat = defaultDateFormat;
            this.timeZone = timeZone;
        }

        public Parser(Settings settings, String componentPrefix) {
            this(defaultDateFormat(settings, componentPrefix), timeZone(settings, componentPrefix));
        }

        public DynamicIndexName parse(String template) {
            return parse(template, null);
        }

        public DynamicIndexName parse(String template, @Nullable DateTimeZone timeZone) {
            DateTimeZone tz = timeZone != null ? timeZone : this.timeZone;
            if (template == null) {
                return null;
            }
            if (!template.startsWith(EXPRESSION_LEFT_BOUND) || !template.endsWith(EXPRESSION_RIGHT_BOUND)) {
                return new DynamicIndexName(template, new StaticExpression(template));
            }
            return new DynamicIndexName(template, CompoundExpression.parse(defaultDateFormat, tz, template.toCharArray(), 1, template.length() - 1));
        }

        public DynamicIndexName[] parse(String[] templates) {
            return parse(templates, null);
        }

        public DynamicIndexName[] parse(String[] templates, @Nullable DateTimeZone timeZone) {
            if (templates.length == 0) {
                return null;
            }
            DynamicIndexName[] dynamicIndexNames = new DynamicIndexName[templates.length];
            for (int i = 0; i < dynamicIndexNames.length; i++) {
                dynamicIndexNames[i] = parse(templates[i], timeZone);
            }
            return dynamicIndexNames;
        }
    }
}
