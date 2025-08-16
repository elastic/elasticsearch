/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class PatternedTextValueProcessor {
    private static final String TEXT_ARG_PLACEHOLDER = "%W";
    private static final String TIMESTAMP_PLACEHOLDER = "%T";
    private static final String DELIMITER = "[\\s\\[\\]]";
    private static final String SPACE = " ";

    // 2021-04-13T13:51:38.000Z
    private static final Pattern timestampPattern = Pattern.compile(
            "^(\\d{4})[-/](\\d{2})[-/](\\d{2})[T ](\\d{2}):(\\d{2}):(\\d{2})([.,](\\d{3}|\\d{6})Z?)?[ ]?([+\\-]\\d{2}([:]?\\d{2})?)?$"
    );
    private static final Pattern letterTimestamp = Pattern.compile(
            "^(\\d{2})[ /](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[ /](\\d{4})[ :](\\d{2}):(\\d{2}):(\\d{2})$"
    );

    record Parts(String template, Long timestamp, String templateId, List<String> args) {
        Parts(String template, Long timestamp, List<String> args) {
            this(template, timestamp, PatternedTextValueProcessor.templateId(template), args);
        }
    }

    static String templateId(String template) {
        byte[] bytes = template.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
        byte[] hashBytes = new byte[8];
        ByteUtils.writeLongLE(hash.h1, hashBytes, 0);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(hashBytes);
    }

    static Parts split(String text) {
        StringBuilder template = new StringBuilder();
        List<String> args = new ArrayList<>();
        String[] tokens = text.split(DELIMITER);
        Long timestamp = null;
        int textIndex = 0;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.isEmpty()) {
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
                continue;
            }

            Tuple<Long, Integer> ts = null;
            if (timestamp == null && (ts = parse(tokens, i)) != null) {
                timestamp = ts.v1();
                if (ts.v2() == 2) {
                    textIndex += tokens[i + 1].length() + 1;
                    i++;
                } else if (ts.v2() == 4) {
                    textIndex += tokens[i + 1].length() + 1;
                    textIndex += tokens[i + 2].length() + 1;
                    textIndex += tokens[i + 3].length() + 1;
                    i+=3;
                }
                template.append(TIMESTAMP_PLACEHOLDER);
            } else if (isArg(token)) {
                args.add(token);
                template.append(TEXT_ARG_PLACEHOLDER);
            } else {
                template.append(token);
            }

            textIndex += token.length();
            if (textIndex < text.length()) {
                template.append(text.charAt(textIndex++));
            }
        }
        while (textIndex < text.length()) {
            template.append(text.charAt(textIndex++));
        }
        return new Parts(template.toString(), timestamp, args);
    }

    private static boolean isArg(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isDigit(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static String merge(Parts parts) {
        StringBuilder builder = new StringBuilder();
        String[] templateParts = parts.template.split(DELIMITER);
        int i = 0;
        int templateIndex = 0;
        for (String part : templateParts) {
            if (part.equals(TEXT_ARG_PLACEHOLDER)) {
                builder.append(parts.args.get(i++));
                templateIndex += TEXT_ARG_PLACEHOLDER.length();
            } else if (part.equals(TIMESTAMP_PLACEHOLDER)) {
                assert parts.timestamp != null;
                builder.append(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(parts.timestamp));
                templateIndex += TIMESTAMP_PLACEHOLDER.length();
            } else if (part.isEmpty() == false) {
                builder.append(part);
                templateIndex += part.length();
            }
            if (templateIndex < parts.template.length()) {
                builder.append(parts.template.charAt(templateIndex++));
            }
        }
        assert i == parts.args.size() : "expected " + i + " but got " + parts.args.size();
        assert builder.toString().contains(TEXT_ARG_PLACEHOLDER) == false : builder.toString();
        while (templateIndex < parts.template.length()) {
            builder.append(parts.template.charAt(templateIndex++));
        }
        return builder.toString();
    }

    static String encodeRemainingArgs(Parts parts) {
        return String.join(SPACE, parts.args);
    }

    static List<String> decodeRemainingArgs(String mergedArgs) {
        return Arrays.asList(mergedArgs.split(SPACE));
    }

    static int countArgs(String template) {
        int count = 0;
        for (int i = 0; i < template.length() - 1; i++) {
            if (template.charAt(i) == '%') {
                char next = template.charAt(i + 1);
                if (next == 'W') {
                    count++;
                    i++;
                }
            }
        }
        return count;
    }

    static boolean hasTimestamp(String template) {
        return template.contains(TIMESTAMP_PLACEHOLDER);
    }

    public static boolean isSingleTokenTimestamp(String text) {
        return timestampPattern.matcher(text).matches();
    }

    public static boolean isTwoTokenTimestamp(String[] tokens, int i) {
        String token = tokens[i];
        return i < tokens.length - 1
                && token.length() == 10
                && tokens[i + 1].length() >= 8
                && tokens[i + 1].length() < 16
                && isSingleTokenTimestamp(tokens[i] + SPACE + tokens[i + 1]);
    }

    public static long parseTwoToken(String[] tokens, int i) {
        String combined = tokens[i].replace("/", "-") + 'T' + tokens[i + 1];
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(combined);
    }


    static DateFormatter letterDateFormat = DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss");
    public static Tuple<Long, Integer> parse(String[] tokens, int i) {

        if (isSingleTokenTimestamp(tokens[i])) {
            try {
                return Tuple.tuple(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(tokens[i]), 1);
            } catch (Exception ignored) {

            }
        }

        try {
            return Tuple.tuple(letterDateFormat.parseMillis(tokens[i]), 1);
        } catch (Exception ignored) {

        }

        try {
            final DateFormatter dateFormatter = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
            return Tuple.tuple(dateFormatter.parseMillis(tokens[i]), 1);
        } catch (Exception ignored) {

        }

        if (isTwoTokenTimestamp(tokens, i)) {
            try {
                String combined = tokens[i].replace("/", "-") + 'T' + tokens[i + 1];
                return Tuple.tuple(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(combined), 2);
            } catch (Exception ignored) {

            }
        }

        String token = tokens[i];
        if (i < tokens.length - 3
                && token.length() == 2
                && tokens[i + 1].length() == 3
                && tokens[i + 2].length() == 4
                && tokens[i + 3].length() == 12) {
            String combined = String.join(" ", Arrays.copyOfRange(tokens, i, i + 4));
            try {
                final DateFormatter dateFormatter = DateFormatter.forPattern("dd MMM yyyy HH:mm:ss.SSS");
                return Tuple.tuple(dateFormatter.parseMillis(combined), 4);
            } catch (Exception ignored) {
            }
        }


        return null;
    }

}
