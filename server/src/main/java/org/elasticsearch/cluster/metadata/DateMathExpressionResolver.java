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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class DateMathExpressionResolver implements ExpressionResolver {

    private static final DateFormatter DEFAULT_DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
    private static final String EXPRESSION_LEFT_BOUND = "<";
    private static final String EXPRESSION_RIGHT_BOUND = ">";
    private static final char LEFT_BOUND = '{';
    private static final char RIGHT_BOUND = '}';
    private static final char ESCAPE_CHAR = '\\';
    private static final char TIME_ZONE_BOUND = '|';

    public static final DateMathExpressionResolver INSTANCE = new DateMathExpressionResolver();

    private DateMathExpressionResolver() {}

    @Override
    public List<String> resolve(final Context context, List<String> expressions) {
        List<String> result = new ArrayList<>(expressions.size());
        for (String expression : expressions) {
            result.add(resolveExpression(expression, context));
        }
        return result;
    }

    @SuppressWarnings("fallthrough")
    String resolveExpression(String expression, final Context context) {
        if (expression.startsWith(EXPRESSION_LEFT_BOUND) == false || expression.endsWith(EXPRESSION_RIGHT_BOUND) == false) {
            return expression;
        }

        boolean escape = false;
        boolean inDateFormat = false;
        boolean inPlaceHolder = false;
        final StringBuilder beforePlaceHolderSb = new StringBuilder();
        StringBuilder inPlaceHolderSb = new StringBuilder();
        final char[] text = expression.toCharArray();
        final int from = 1;
        final int length = text.length - 1;
        for (int i = from; i < length; i++) {
            boolean escapedChar = escape;
            if (escape) {
                escape = false;
            }

            char c = text[i];
            if (c == ESCAPE_CHAR) {
                if (escapedChar) {
                    beforePlaceHolderSb.append(c);
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
                            inPlaceHolderSb.append(c);
                        } else if (!inDateFormat) {
                            inDateFormat = true;
                            inPlaceHolderSb.append(c);
                        } else {
                            throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                " invalid character in placeholder at position [{}]", new String(text, from, length), i);
                        }
                        break;

                    case RIGHT_BOUND:
                        if (inDateFormat && escapedChar) {
                            inPlaceHolderSb.append(c);
                        } else if (inDateFormat) {
                            inDateFormat = false;
                            inPlaceHolderSb.append(c);
                        } else {
                            String inPlaceHolderString = inPlaceHolderSb.toString();
                            int dateTimeFormatLeftBoundIndex = inPlaceHolderString.indexOf(LEFT_BOUND);
                            String mathExpression;
                            String dateFormatterPattern;
                            DateFormatter dateFormatter;
                            final ZoneId timeZone;
                            if (dateTimeFormatLeftBoundIndex < 0) {
                                mathExpression = inPlaceHolderString;
                                dateFormatter = DEFAULT_DATE_FORMATTER;
                                timeZone = ZoneOffset.UTC;
                            } else {
                                if (inPlaceHolderString.lastIndexOf(RIGHT_BOUND) != inPlaceHolderString.length() - 1) {
                                    throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing closing `}`" +
                                        " for date math format", inPlaceHolderString);
                                }
                                if (dateTimeFormatLeftBoundIndex == inPlaceHolderString.length() - 2) {
                                    throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing date format",
                                        inPlaceHolderString);
                                }
                                mathExpression = inPlaceHolderString.substring(0, dateTimeFormatLeftBoundIndex);
                                String patternAndTZid =
                                    inPlaceHolderString.substring(dateTimeFormatLeftBoundIndex + 1, inPlaceHolderString.length() - 1);
                                int formatPatternTimeZoneSeparatorIndex = patternAndTZid.indexOf(TIME_ZONE_BOUND);
                                if (formatPatternTimeZoneSeparatorIndex != -1) {
                                    dateFormatterPattern = patternAndTZid.substring(0, formatPatternTimeZoneSeparatorIndex);
                                    timeZone = DateUtils.of(patternAndTZid.substring(formatPatternTimeZoneSeparatorIndex + 1));
                                } else {
                                    dateFormatterPattern = patternAndTZid;
                                    timeZone = ZoneOffset.UTC;
                                }
                                dateFormatter = DateFormatter.forPattern(dateFormatterPattern);
                            }

                            DateFormatter formatter = dateFormatter.withZone(timeZone);
                            DateMathParser dateMathParser = formatter.toDateMathParser();
                            Instant instant = dateMathParser.parse(mathExpression, context::getStartTime, false, timeZone);

                            String time = formatter.format(instant);
                            beforePlaceHolderSb.append(time);
                            inPlaceHolderSb = new StringBuilder();
                            inPlaceHolder = false;
                        }
                        break;

                    default:
                        inPlaceHolderSb.append(c);
                }
            } else {
                switch (c) {
                    case LEFT_BOUND:
                        if (escapedChar) {
                            beforePlaceHolderSb.append(c);
                        } else {
                            inPlaceHolder = true;
                        }
                        break;

                    case RIGHT_BOUND:
                        if (!escapedChar) {
                            throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                " invalid character at position [{}]. `{` and `}` are reserved characters and" +
                                " should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)",
                                new String(text, from, length), i);
                        }
                    default:
                        beforePlaceHolderSb.append(c);
                }
            }
        }

        if (inPlaceHolder) {
            throw new ElasticsearchParseException("invalid dynamic name expression [{}]. date math placeholder is open ended",
                new String(text, from, length));
        }
        if (beforePlaceHolderSb.length() == 0) {
            throw new ElasticsearchParseException("nothing captured");
        }
        return beforePlaceHolderSb.toString();
    }
}
