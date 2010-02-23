/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util;

import org.apache.lucene.util.UnicodeUtil;

import java.util.Arrays;

/**
 * @author kimchy (shay.banon)
 */
public class Unicode {

    private static ThreadLocal<UnicodeUtil.UTF8Result> cachedUtf8Result = new ThreadLocal<UnicodeUtil.UTF8Result>() {
        @Override protected UnicodeUtil.UTF8Result initialValue() {
            return new UnicodeUtil.UTF8Result();
        }
    };

    private static ThreadLocal<UnicodeUtil.UTF16Result> cachedUtf16Result = new ThreadLocal<UnicodeUtil.UTF16Result>() {
        @Override protected UnicodeUtil.UTF16Result initialValue() {
            return new UnicodeUtil.UTF16Result();
        }
    };

    public static byte[] fromStringAsBytes(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = unsafeFromStringAsUtf8(source);
        return Arrays.copyOfRange(result.result, 0, result.length);
    }

    public static UnicodeUtil.UTF8Result fromStringAsUtf8(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = new UnicodeUtil.UTF8Result();
        UnicodeUtil.UTF16toUTF8(source, 0, source.length(), result);
        return result;
    }

    public static UnicodeUtil.UTF8Result unsafeFromStringAsUtf8(String source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = cachedUtf8Result.get();
        UnicodeUtil.UTF16toUTF8(source, 0, source.length(), result);
        return result;
    }

    public static String fromBytes(byte[] source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF16Result result = unsafeFromBytesAsUtf16(source);
        return new String(result.result, 0, result.length);
    }

    public static UnicodeUtil.UTF16Result fromBytesAsUtf16(byte[] source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF16Result result = new UnicodeUtil.UTF16Result();
        UnicodeUtil.UTF8toUTF16(source, 0, source.length, result);
        return result;
    }

    public static UnicodeUtil.UTF16Result unsafeFromBytesAsUtf16(byte[] source) {
        if (source == null) {
            return null;
        }
        UnicodeUtil.UTF16Result result = cachedUtf16Result.get();
        UnicodeUtil.UTF8toUTF16(source, 0, source.length, result);
        return result;
    }

}
