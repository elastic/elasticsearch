/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 *  Describes the type and location of an argument in the template. A list of argument infos is encoded and stored in a doc value
 *  column, this is used to re-combine the template and argument columns. Documents with identical templates share the same
 *  of argument infos, and since indices are sorted by template_id, this doc value column compresses very well.
 */
public class Arg {

    private static final String SPACE = " ";
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    static final int VINT_MAX_BYTES = 5;

    public enum Type {
        GENERIC(0);

        private final int code;
        private static final Type[] lookup = new Type[values().length];
        static {
            for (var type : values()) {
                lookup[type.code] = type;
            }
        }

        Type(int code) {
            this.code = code;
        }

        public int toCode() {
            return code;
        }

        public static Type fromCode(int code) {
            return lookup[code];
        }
    }

    record Info(Type type, int offsetInTemplate) {
        public Info {
            assert offsetInTemplate >= 0;
        }

        void writeTo(ByteArrayDataOutput out, int previousOffset) throws IOException {
            out.writeVInt(type.toCode());
            int diff = offsetInTemplate - previousOffset;
            out.writeVInt(diff);
        }

        static Info readFrom(DataInput in, int previousOffset) throws IOException {
            var type = Type.fromCode(in.readVInt());
            int diffFromPrevious = in.readVInt();
            int offsetInfoTemplate = previousOffset + diffFromPrevious;
            return new Info(type, offsetInfoTemplate);
        }
    }

    static boolean isArg(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isDigit(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static String encodeInfo(List<Info> arguments) throws IOException {
        int maxSize = VINT_MAX_BYTES + arguments.size() * (VINT_MAX_BYTES + VINT_MAX_BYTES);
        byte[] buffer = new byte[maxSize];
        var dataInput = new ByteArrayDataOutput(buffer);
        dataInput.writeVInt(arguments.size());
        int previousOffset = 0;
        for (var arg : arguments) {
            arg.writeTo(dataInput, previousOffset);
            previousOffset = arg.offsetInTemplate;
        }

        int size = dataInput.getPosition();
        byte[] data = Arrays.copyOfRange(buffer, 0, size);
        return ENCODER.encodeToString(data);
    }

    /**
     * Byte-level equivalent of {@link #encodeInfo(List)} for the columnar path: writes base64url
     * bytes into {@code dest} and returns their length.
     *
     * <p>{@code out} is a caller-owned, reusable {@link ByteArrayDataOutput}, reset onto
     * {@code rawScratch} on every call so that it is not allocated per document.
     */
    static int encodeInfoBytes(int[] offsets, int count, ByteArrayDataOutput out, byte[] rawScratch, byte[] dest) throws IOException {
        out.reset(rawScratch);
        out.writeVInt(count);
        int prev = 0;
        for (int i = 0; i < count; i++) {
            out.writeVInt(Type.GENERIC.toCode()); // always 0
            out.writeVInt(offsets[i] - prev);
            prev = offsets[i];
        }
        int rawLen = out.getPosition();
        // Encode only the written prefix [0, rawLen). Arrays.copyOfRange allocates a small
        // (<100 bytes for typical log lines) intermediate array — the larger allocation savings
        // come from the caller avoiding String, Pattern.split, ArrayList, and StringBuilder.
        return ENCODER.encode(Arrays.copyOfRange(rawScratch, 0, rawLen), dest);
    }

    static int argsInfoMaxBase64Bytes(int count) {
        int rawMax = VINT_MAX_BYTES + count * 2 * VINT_MAX_BYTES;
        return ((rawMax + 2) / 3) * 4;
    }

    static List<Info> decodeInfo(String encoded) throws IOException {
        byte[] encodedBytes = DECODER.decode(encoded);
        var input = new ByteArrayDataInput(encodedBytes);

        int numArgs = input.readVInt();
        int previousOffset = 0;
        List<Info> arguments = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; i++) {
            var argInfo = Info.readFrom(input, previousOffset);
            arguments.add(argInfo);
            previousOffset = argInfo.offsetInTemplate;
        }
        return arguments;
    }

    static String encodeRemainingArgs(PatternTextValueProcessor.Parts parts) {
        return String.join(SPACE, parts.args());
    }

    static String[] decodeRemainingArgs(String mergedArgs) {
        return mergedArgs.split(SPACE);
    }
}
