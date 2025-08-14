/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class Arg {

    private static final String SPACE = " ";

    public enum Type {
        GENERAL(0),
        IP4(1),
        INTEGER(2);

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

    record Schema(Type type, int offsetFromPrevArg) {
        public Schema {
            assert offsetFromPrevArg >= 0;
        }
        void writeTo(ByteArrayDataOutput out) throws IOException {
            out.writeVInt(type.toCode());
            out.writeVInt(offsetFromPrevArg);
        }

        static Schema readFrom(DataInput in) throws IOException {
            return new Schema(Type.fromCode(in.readVInt()), in.readVInt());
        }
    }

    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static int VINT_MAX_BYTES = 5;

    public static String encodeSchema(List<Schema> arguments) throws IOException {
        int maxSize = VINT_MAX_BYTES  + arguments.size() * (VINT_MAX_BYTES + VINT_MAX_BYTES);
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

    public static List<Schema> decodeSchema(String encoded) throws IOException {
        byte[] encodedBytes = DECODER.decode(encoded);
        var input = new ByteArrayDataInput(encodedBytes);

        int numArgs = input.readVInt();
        List<Schema> arguments = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; i++) {
            arguments.add(Schema.readFrom(input));
        }
        return arguments;
    }

    static boolean isArg(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isDigit(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static String encodeRemainingArgs(PatternedTextValueProcessor.Parts parts) {
        return String.join(SPACE, parts.args());
    }

    static String[] decodeRemainingArgs(String mergedArgs) {
        return mergedArgs.split(SPACE);
    }
}
