/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.elasticsearch.grok.GrokCaptureConfig.NativeExtracterMap;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * The type defined for the field in the pattern.
 */
enum GrokCaptureType {
    STRING {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forString(emit -> rawExtracter(backRefs, emit));
        }
    },
    INTEGER {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forInt(emit -> rawExtracter(backRefs, str -> emit.accept(Integer.parseInt(str))));
        }
    },
    LONG {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forLong(emit -> rawExtracter(backRefs, str -> emit.accept(Long.parseLong(str))));
        }
    },
    FLOAT {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forFloat(emit -> rawExtracter(backRefs, str -> emit.accept(Float.parseFloat(str))));
        }
    },
    DOUBLE {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forDouble(emit -> rawExtracter(backRefs, str -> emit.accept(Double.parseDouble(str))));
        }
    },
    BOOLEAN {
        @Override
        <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map) {
            return map.forBoolean(emit -> rawExtracter(backRefs, str -> emit.accept(Boolean.parseBoolean(str))));
        }
    };

    abstract <T> T nativeExtracter(int[] backRefs, NativeExtracterMap<T> map);

    static GrokCaptureType fromString(String str) {
        return switch (str) {
            case "string" -> STRING;
            case "int" -> INTEGER;
            case "long" -> LONG;
            case "float" -> FLOAT;
            case "double" -> DOUBLE;
            case "boolean" -> BOOLEAN;
            default -> STRING;
        };
    }

    protected static GrokCaptureExtracter rawExtracter(int[] backRefs, Consumer<? super String> emit) {
        return (utf8Bytes, offset, region) -> {
            for (int number : backRefs) {
                if (region.beg[number] >= 0) {
                    int matchOffset = offset + region.beg[number];
                    int matchLength = region.end[number] - region.beg[number];
                    emit.accept(new String(utf8Bytes, matchOffset, matchLength, StandardCharsets.UTF_8));
                }
            }
        };
    }
}
