/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class PatternedTextValueProcessor {
    private static final String TEXT_ARG_PLACEHOLDER = "%W";
    private static final String DATE_ARG_PLACEHOLDER = "%D";
    private static final String IP_ARG_PLACEHOLDER = "%I";
    private static final String UUID_ARG_PLACEHOLDER = "%U";
    private static final String TIMESTAMP_PLACEHOLDER = "%T";
    private static final String DELIMITER = "[\\s\\[\\]]";
    private static final String SPACE = " ";

    // 2021-04-13T13:51:38.000Z
    private static final Pattern timestampPattern = Pattern.compile(
        "^(\\d{4})[-/](\\d{2})[-/](\\d{2})[T ](\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{3})Z?)?[ ]?([\\+\\-]\\d{2}([:]?\\d{2})?)?$"
    );

    record Parts(String template, Long timestamp, List<String> args, String indexed) {
        String templateStripped() {
            List<String> stripped = new ArrayList<>();
            String[] parts = template.split(SPACE);
            for (String part : parts) {
                if (part.startsWith("%") == false) {
                    stripped.add(part);
                }
            }
            return String.join(SPACE, stripped);
        }

    }

    static Parts split(String text) {
        StringBuilder template = new StringBuilder();
        StringBuilder indexed = new StringBuilder();
        Long timestamp = null;
        List<String> args = new ArrayList<>();
        byte[] ipv4Bytes = new byte[4];
        byte[] uuidBytes = new byte[16];
        String[] tokens = text.split(DELIMITER);
        int textIndex = 0;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.isEmpty()) {
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
                continue;
            }
            if (isTimestamp(tokens[i])) {
                long millis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(token);
                if (timestamp == null) {
                    timestamp = millis;
                    template.append(TIMESTAMP_PLACEHOLDER);
                } else {
                    byte[] millisBytes = new byte[8];
                    ByteUtils.writeLongLE(millis, millisBytes, 0);
                    String encoded = Base64.getEncoder().withoutPadding().encodeToString(millisBytes);
                    args.add(encoded);
                    template.append(DATE_ARG_PLACEHOLDER);
                    indexed.append(encoded).append(SPACE);
                }
            } else if (i < tokens.length - 1
                && token.length() == 10
                && tokens[i + 1].length() >= 8
                && tokens[i + 1].length() < 16
                && isTimestamp(tokens[i] + SPACE + tokens[i + 1])) {
                    String combined = tokens[i].replace("/", "-") + 'T' + tokens[i + 1];
                    long millis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(combined);
                    if (timestamp == null) {
                        timestamp = millis;
                        template.append(TIMESTAMP_PLACEHOLDER);
                        textIndex += tokens[i + 1].length() + 1;
                        i++;
                    } else {
                        byte[] millisBytes = new byte[8];
                        ByteUtils.writeLongLE(millis, millisBytes, 0);
                        String encoded = Base64.getEncoder().withoutPadding().encodeToString(millisBytes);
                        args.add(encoded);
                        template.append(DATE_ARG_PLACEHOLDER);
                        indexed.append(encoded).append(SPACE);
                        textIndex += tokens[i + 1].length() + 1;
                        i++;
                        if (i < tokens.length - 1 && tokens[i + 1].equals("+0000")) {
                            textIndex += tokens[i + 1].length() + 1;
                            i++;
                        }
                    }
                } else if (isIpv4(token, ipv4Bytes)) {
                    String encoded = Base64.getEncoder().withoutPadding().encodeToString(ipv4Bytes);
                    args.add(encoded);
                    template.append(IP_ARG_PLACEHOLDER);
                    indexed.append(encoded).append(SPACE);
                } else if (isUUID(token, uuidBytes)) {
                    String encoded = Base64.getEncoder().withoutPadding().encodeToString(uuidBytes);
                    args.add(encoded);
                    template.append(UUID_ARG_PLACEHOLDER);
                    indexed.append(encoded).append(SPACE);
                } else if (isArg(token)) {
                    args.add(token);
                    template.append(TEXT_ARG_PLACEHOLDER);
                    indexed.append(token).append(SPACE);
                } else {
                    template.append(token);
                    indexed.append(token).append(SPACE);
                }
            textIndex += token.length();
            if (textIndex < text.length()) {
                template.append(text.charAt(textIndex++));
            }
        }
        while (textIndex < text.length()) {
            template.append(text.charAt(textIndex++));
        }
        return new Parts(template.toString(), timestamp, args, indexed.toString().trim());
    }

    private static boolean isTimestamp(String text) {
        return timestampPattern.matcher(text).matches();
    }

    /**
     * Checks if the given text is a valid IPv4 address and fills the provided byte array with the corresponding bytes.
     * If the text is not a valid IPv4 address, it returns false and the byte array's content is not undefined and should not be used.
     * @param text the text to check
     * @param bytes the byte array to fill with the parsed UUID bytes
     * @return true if the text is a valid IPv4 address, false otherwise
     */
    static boolean isIpv4(String text, byte[] bytes) {
        if (text.length() < 7 || text.length() > 15) {
            return false;
        }
        int octetIndex = 0;
        int octetValue = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '.') {
                if (octetIndex == 3) {
                    return false;
                }
                bytes[octetIndex] = (byte) octetValue;
                octetValue = 0;
                octetIndex++;
            } else if ('0' <= c && c <= '9') {
                // Character.isDigit(c) is invalid for IPs and inconsistent with the calculation of the numeric value of the character
                octetValue = octetValue * 10 + c - '0';
                if (octetValue > 255) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (octetIndex != 3) {
            return false;
        }
        bytes[octetIndex] = (byte) octetValue;
        return true;
    }

    private static String toIPv4(byte[] bytes) {
        assert bytes.length == 4 : bytes.length;
        return Byte.toUnsignedInt(bytes[0])
            + "."
            + Byte.toUnsignedInt(bytes[1])
            + "."
            + Byte.toUnsignedInt(bytes[2])
            + "."
            + Byte.toUnsignedInt(bytes[3]);
    }

    static boolean isUUID(String text, byte[] bytes) {
        assert bytes.length == 16 : bytes.length;
        if (text.length() == 36 && text.charAt(8) == '-' && text.charAt(13) == '-' && text.charAt(18) == '-' && text.charAt(23) == '-') {
            UUID uuid;
            try {
                uuid = UUID.fromString(text);
            } catch (IllegalArgumentException e) {
                // false positive in the enclosing if statement - should be very rare. Just ignore it.
                return false;
            }
            ByteUtils.writeLongLE(uuid.getMostSignificantBits(), bytes, 0);
            ByteUtils.writeLongLE(uuid.getLeastSignificantBits(), bytes, 8);
            return true;
        }
        return false;
    }

    private static String toUUID(byte[] bytes) {
        assert bytes.length == 16 : bytes.length;
        UUID uuid = new UUID(ByteUtils.readLongLE(bytes, 0), ByteUtils.readLongLE(bytes, 8));
        return uuid.toString();
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
            } else if (part.equals(DATE_ARG_PLACEHOLDER)) {
                var bytes = Base64.getDecoder().decode(parts.args.get(i++));
                builder.append(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(ByteUtils.readLongLE(bytes, 0)));
                templateIndex += DATE_ARG_PLACEHOLDER.length();
            } else if (part.equals(IP_ARG_PLACEHOLDER)) {
                var bytes = Base64.getDecoder().decode(parts.args.get(i++));
                builder.append(toIPv4(bytes));
                templateIndex += IP_ARG_PLACEHOLDER.length();
            } else if (part.equals(UUID_ARG_PLACEHOLDER)) {
                var bytes = Base64.getDecoder().decode(parts.args.get(i++));
                builder.append(toUUID(bytes));
                templateIndex += UUID_ARG_PLACEHOLDER.length();
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

    static String mergeRemainingArgs(Parts parts, int startOffset) {
        StringBuilder builder = new StringBuilder();
        for (int i = startOffset; i < parts.args.size(); i++) {
            builder.append((i > startOffset) ? SPACE : "").append(parts.args.get(i));
        }
        return builder.toString();
    }

    static void addRemainingArgs(List<String> args, String mergedArgs) {
        Collections.addAll(args, mergedArgs.split(SPACE));
    }

    static int countArgs(String template) {
        int count = 0;
        for (int i = 0; i < template.length() - 1; i++) {
            if (template.charAt(i) == '%') {
                char next = template.charAt(i + 1);
                if (next == 'W' || next == 'D' || next == 'U' || next == 'I') {
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
}
