/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class PatternedTextValueProcessor {
    private static final String DELIMITER = "[\\s\\[\\]]";
    private static final String SPACE = " ";

    public record Parts(String template, String templateId, List<String> args, List<ArgSchema> schemas) {

        Parts(String template, List<String> args) {
            this(template, PatternedTextValueProcessor.templateId(template), args, null);
        }

        Parts(String template, List<String> args, List<ArgSchema> schemas) {
            this(template, PatternedTextValueProcessor.templateId(template), args, schemas);
        }

        Parts(String template, List<String> args, String encodedArgsSchema) throws IOException {
            this(template, PatternedTextValueProcessor.templateId(template), args, decodeArgumentSchema(encodedArgsSchema));
        }

        public int messageSize() {
            int size = template.length();
            for (var arg : args) {
                size += arg.length();
            }
            return size;
        }
    }

    public enum ArgType {
        GENERAL(0),
        IP4(1),
        INTEGER(2);

        private final int code;
        private static final ArgType[] lookup = new ArgType[values().length];
        static {
            for (ArgType type : values()) {
                lookup[type.code] = type;
            }
        }

        ArgType(int code) {
            this.code = code;
        }

        public int toCode() {
            return code;
        }

        public static ArgType fromCode(int code) {
            return lookup[code];
        }
    }

    record ArgSchema(ArgType type, int offsetFromPrevArg) {
        void writeTo(ByteArrayDataOutput out) throws IOException {
            out.writeVInt(type.toCode());
            out.writeVInt(offsetFromPrevArg);
        }

        static ArgSchema readFrom(ByteArrayDataInput in) {
            return new ArgSchema(ArgType.fromCode(in.readVInt()), in.readVInt());
        }
    }

    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    public static String encodeArgumentSchema(List<ArgSchema> arguments) throws IOException {
        int maxSize = Integer.BYTES + arguments.size() * (Integer.BYTES + Integer.BYTES);
        byte[] buffer = new byte[maxSize];
        var dataInput = new ByteArrayDataOutput(buffer);
        dataInput.writeVInt(arguments.size());
        for (var arg : arguments) {
            arg.writeTo(dataInput);
        }

        int size = dataInput.getPosition();
        byte[] data = Arrays.copyOfRange(buffer, 0, size);
        return ENCODER.encodeToString(data);
    }

    public static List<ArgSchema> decodeArgumentSchema(String encoded) throws IOException {
        byte[] encodedBytes = DECODER.decode(encoded);
        var input = new ByteArrayDataInput(encodedBytes);

        int numArgs = input.readVInt();
        List<ArgSchema> arguments = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; i++) {
            arguments.add(ArgSchema.readFrom(input));
        }
        return arguments;
    }

    static String templateId(String template) {
        byte[] bytes = template.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
        byte[] hashBytes = new byte[8];
        ByteUtils.writeLongLE(hash.h1, hashBytes, 0);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(hashBytes);
    }

    public static Parts split(String text) throws IOException {
        StringBuilder template = new StringBuilder(text.length());
        List<String> args = new ArrayList<>();
        List<ArgSchema> schemas = new ArrayList<>();
        String[] tokens = text.split(DELIMITER);
        int textIndex = 0;
        int prevArgOffset = 0;
        for (String token : tokens) {
            if (token.isEmpty()) {
                // add the previous delimiter
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
            } else {
                if (isArg(token)) {
                    args.add(token);
                    schemas.add(new ArgSchema(ArgType.GENERAL, template.length() - prevArgOffset));
                    prevArgOffset = template.length();
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
        return new Parts(template.toString(), args, schemas);
    }

    private static boolean isArg(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isDigit(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static String merge(Parts parts) {
        StringBuilder builder = new StringBuilder(parts.messageSize());
        int numArgs = parts.args.size();

        int offsetInTemplate = 0;
        int nextToWrite = 0;
        for (int i = 0; i < numArgs; i++) {
            String arg = parts.args.get(i);
            ArgSchema argSchema = parts.schemas.get(i);

            offsetInTemplate += argSchema.offsetFromPrevArg;
            builder.append(parts.template, nextToWrite, offsetInTemplate);
            builder.append(arg);
            nextToWrite = offsetInTemplate;
        }

        if (nextToWrite < parts.template.length()) {
            builder.append(parts.template.substring(nextToWrite));
        }
        return builder.toString();
    }

    static String encodeRemainingArgs(Parts parts) {
        return String.join(SPACE, parts.args);
    }

    static List<String> decodeRemainingArgs(String mergedArgs) {
        return Arrays.asList(mergedArgs.split(SPACE));
    }
}
