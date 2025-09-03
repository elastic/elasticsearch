/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PatternedTextValueProcessor {
    private static final String DELIMITER = "[\\s\\[\\]]";

    public record Parts(String template, String templateId, List<String> args, List<Arg.Info> argsInfo) {
        Parts(String template, List<String> args, List<Arg.Info> argsInfo) {
            this(template, PatternedTextValueProcessor.templateId(template), args, argsInfo);
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

    static Parts split(String text) throws IOException {
        StringBuilder template = new StringBuilder(text.length());
        List<String> args = new ArrayList<>();
        List<Arg.Info> argsInfo = new ArrayList<>();
        String[] tokens = text.split(DELIMITER);
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
        return new Parts(template.toString(), args, argsInfo);
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
