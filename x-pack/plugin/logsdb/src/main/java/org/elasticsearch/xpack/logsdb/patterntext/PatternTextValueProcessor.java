/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PatternTextValueProcessor {
    private static final Pattern DELIMITER = Pattern.compile("[\\s\\[\\]]");
    public static final int MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE = 8 * 1024;

    public record Parts(String template, String templateId, List<String> args, List<Arg.Info> argsInfo, boolean useStoredField) {
        Parts(String template, List<String> args, List<Arg.Info> argsInfo) {
            this(template, PatternTextValueProcessor.templateId(template), args, argsInfo, false);
        }

        static Parts lengthExceeded(String template) {
            return new Parts(template, PatternTextValueProcessor.templateId(template), null, null, true);
        }
    }

    public static int originalSize(String template, String[] args) {
        int size = template.length();
        for (var arg : args) {
            size += arg.length();
        }
        return size;
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
        if (text.length() > MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE) {
            return splitInternal(CharBuffer.wrap(text).subSequence(0, MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE), true);
        } else {
            return splitInternal(text, false);
        }
    }

    static Parts splitInternal(CharSequence text, boolean exceedsMaxLength) {
        StringBuilder template = new StringBuilder(text.length());
        List<String> args = new ArrayList<>();
        List<Arg.Info> argsInfo = new ArrayList<>();
        String[] tokens = DELIMITER.split(text);
        int textIndex = 0;
        for (String token : tokens) {
            if (token.isEmpty()) {
                // add the previous delimiter
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
            } else {
                if (Arg.isArg(token)) {
                    args.add(token);
                    argsInfo.add(new Arg.Info(Arg.Type.GENERIC, template.length()));
                } else {
                    template.append(token);
                }
                textIndex += token.length();
                if (textIndex < text.length()) {
                    template.append(text.charAt(textIndex++));
                }
            }
        }
        while (textIndex < text.length()) {
            template.append(text.charAt(textIndex++));
        }

        return exceedsMaxLength ? Parts.lengthExceeded(text.toString()) : new Parts(template.toString(), args, argsInfo);
    }

    // For testing
    public static String merge(Parts parts) {
        return merge(parts.template, parts.args.toArray(String[]::new), parts.argsInfo);
    }

    static String merge(String template, String[] args, List<Arg.Info> argsInfo) {
        StringBuilder builder = new StringBuilder(originalSize(template, args));
        int numArgs = args.length;

        int nextToWrite = 0;
        for (int i = 0; i < numArgs; i++) {
            String arg = args[i];
            var argInfo = argsInfo.get(i);

            builder.append(template, nextToWrite, argInfo.offsetInTemplate());
            builder.append(arg);
            nextToWrite = argInfo.offsetInTemplate();
        }

        if (nextToWrite < template.length()) {
            builder.append(template, nextToWrite, template.length());
        }
        return builder.toString();
    }
}
