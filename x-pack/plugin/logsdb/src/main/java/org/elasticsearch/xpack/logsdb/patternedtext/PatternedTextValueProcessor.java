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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
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

            Tuple<Long, Integer> ts;
            if (timestamp == null && (ts = parse(tokens, i)) != null) {
                timestamp = ts.v1();

                int iInc = 0;
                int numTokensInTimestamp = ts.v2();
                for (int tokenLen = 2; tokenLen <= numTokensInTimestamp; tokenLen++) {
                    textIndex += tokens[i + tokenLen - 1].length() + 1;
                    iInc++;
                }
                i += iInc;
                template.append(TIMESTAMP_PLACEHOLDER);
            } else if (containsDigit(token)) {
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

    public static boolean containsDigit(String text) {
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

    public static Tuple<Long, Integer> parse(String[] tokens, int i) {

        // 5 tokens left
        if (i < tokens.length - 4) {
            // "Sep 6, 2020 08:29:04 AM"
            String combined = String.join(" ", tokens[i], tokens[i + 1], tokens[i + 2], tokens[i + 3], tokens[i + 4]);
            try {
                // I'm not sure if the dates in the dataset having a leading 0 if they are a single digit
                final DateFormatter dateFormatter = DateFormatter.forPattern("MMM d, yyyy hh:mm:ss a");
                return Tuple.tuple(dateFormatter.parseMillis(combined), 5);
            } catch (Exception ignored) {
            }
        }

        // 4 tokens left
        if (i < tokens.length - 3) {
            // "06 Sep 2020 08:29:04.123"
            String combined = String.join(" ", tokens[i], tokens[i + 1], tokens[i + 2], tokens[i + 3]);
            try {
                final DateFormatter dateFormatter = DateFormatter.forPattern("dd MMM yyyy HH:mm:ss.SSS");
                return Tuple.tuple(dateFormatter.parseMillis(combined), 4);
            } catch (Exception ignored) {

            }
        }


        DateTimeFormatter standardBase = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        DateTimeFormatter standardFormatter = new DateTimeFormatterBuilder()
            .append(standardBase)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalStart().appendPattern("XX").optionalEnd()
            .optionalStart().appendLiteral(" ").appendPattern("XX").optionalEnd()
            .optionalStart().appendOffsetId().optionalEnd()
            .optionalStart().appendLiteral(" ").appendPattern("z").optionalEnd()
            .toFormatter();


        DateTimeFormatter spaceBase = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss");
        DateTimeFormatter spaceFormatter = new DateTimeFormatterBuilder()
            .append(spaceBase)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalStart().appendPattern("XX").optionalEnd()
            .optionalStart().appendLiteral(" ").appendPattern("XX").optionalEnd()
            .optionalStart().appendOffsetId().optionalEnd()
            .optionalStart().appendLiteral(" ").appendPattern("z").optionalEnd()
            .toFormatter();

        // 3 token
        if (i < tokens.length - 2) {
            String combined = String.join(" ", tokens[i], tokens[i + 1], tokens[i + 2]);
            try {
                // "2020-09-06 08:29:04 +0000"
                // "2020-09-06 08:29:04 UTC"
                return Tuple.tuple(parseMillis(combined, spaceFormatter), 3);
            } catch (Exception ignored) {
            }
            try {
                return Tuple.tuple(parseMillis(combined, standardFormatter), 3);
            } catch (Exception ignored) {
            }
        }

        // 2 token
        if (i < tokens.length - 1) {
            String combined = String.join(" ", tokens[i], tokens[i + 1]);
            String attempt = combined
                .replace("/", "-")
                .replace(",", ".");
            try {
                // "2020-09-06 08:29:04,123"
                // "2020-09-06 08:29:04.123"
                // "2020-09-06 08:29:04"
                // "2020/09/06 08:29:04"

                long millis = parseMillis(attempt, spaceFormatter);
                return Tuple.tuple(millis, 2);
            } catch (Exception ignored) {
            }

            try {
                long millis = parseMillis(attempt, standardFormatter);
                return Tuple.tuple(millis, 2);
            } catch (Exception ignored) {
            }

            try {
                // "06/Sep/2020:08:29:04 +0000"
                final DateFormatter dateFormatter = DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss XX");
                return Tuple.tuple(dateFormatter.parseMillis(combined), 2);
            } catch (Exception ignored) {
            }
        }

        // 1 token
        String attempt = tokens[i]
            .replace(",", ".");
        try {
            // "2020-09-06T08:29:04.123456"
            // "2020-09-06T08:29:04.123Z"
            // "2020-09-06T08:29:04,123"
            // "2020-09-06T08:29:04.123+00:00"
            // "2020-09-06T08:29:04Z"
            // "2020-09-06T08:29:04+0000"
            // "2020-09-06T08:29:04.123+0000"


            long millis = parseMillis(attempt, spaceFormatter);
            return Tuple.tuple(millis, 1);
        } catch (Exception ignored) {
        }
        try {
            long millis = parseMillis(attempt, standardFormatter);
            return Tuple.tuple(millis, 1);
        } catch (Exception ignored) {
        }

        return null;
    }

    public static long parseMillis(String input, DateTimeFormatter formatter) {
        TemporalAccessor temporalAccessor = formatter.parseBest(
            input,
            ZonedDateTime::from,
            LocalDateTime::from
        );

        if (temporalAccessor.query(TemporalQueries.zoneId()) != null ||
            temporalAccessor.query(TemporalQueries.offset()) != null) {
            return Instant.from(temporalAccessor).toEpochMilli();
        } else {
            return LocalDateTime.from(temporalAccessor).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        }
    }
}
