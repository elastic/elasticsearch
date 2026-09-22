/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared JSON inputs for {@link SimdJsonDirectWalkerTests} and
 * {@link SimdJsonJacksonComparisonTests}. Inputs are chosen for semantic coverage
 * (types, nesting, escapes, field-name shapes) rather than buffer layout.
 */
public final class SimdJsonTestDocuments {

    private SimdJsonTestDocuments() {}

    /**
     * Documents exercised with {@code buffer.length == document.length} (no trailing padding).
     */
    public static List<String> exactBufferLengthDocuments() {
        List<String> docs = new ArrayList<>();
        // tag::noformat
        Collections.addAll(
            docs,
            """
                {}""","""
                {"a":1}""", """
                {"s":"hello"}""", """
                {"b":true}""", """
                {"b":false}""", """
                {"n":null}""", """
                {"d":3.14}""", """
                {"sci":1.5e10}""",  """
                {"neg":-42}""", """
                {"arr":[1,"s",true,null,3.14]}""", """
                {"o":{"inner":"val"}}""", """
                {"esc":"line1\\nline2"}""", """
                {"q":"say \\"hi\\""}""", """
                {"u":"\\u0041"}""",  """
                {"long":9999999999}""", """
                {"last_esc":"a\\nb"}""",
                // Exact 16 / 32 / 64 UTF-8 byte document lengths (whole-document SIMD lane boundaries).
                // Unpadded: natural JSON content sized to the boundary.
                """
                {"a":"12345678"}""", """
                {"name":"012345678901234567890"}""", """
                {"payload":"01234567890123456789012345678901234567890123456789"}""",
                // Padded: insignificant whitespace before '}' (JSON-equivalent, different byte layout).
                """
                {"a":"b"       }""", """
                {"abcd":"efghijklmnopqr"       }""", """
                {"abcdefghijk":"lmnopqrstuvwxyz0123456789ABCDEFGHIJKLMN"       }""",
                // Escaped field names
               """
                {"a\\nb":1}""", """
                {"x\\"y":1}""", """
                {"\\u0041":1}""",
               // Multiple fields; last field name varies in length
                """
                {"first":1,"x":2}""", """
                {"first":1,"ab":2}""", """
                {"first":1,"abcdefg":2}""", """
                {"first":1,"abcdefgh":2}""", """
                {"first":1,"abcdefghi":2}""",
                // Integer digit-count boundaries (1, 2, and 3+ digits; positive, negative, and
                // zero; short mantissa immediately followed by '.'/'e'), and the same values as
                // array elements.
                """
                {"n":0}""", """
                {"n":9}""", """
                {"n":-5}""", """
                {"n":-0}""", """
                {"n":0.5}""", """
                {"n":0e5}""", """
                {"n":0E056}""", """
                {"n":10}""", """
                {"n":99}""", """
                {"n":-99}""", """
                {"n":100}""", """
                {"n":1234567890}""", """
                {"n":9876543210}""", """
                {"n":1.5}""", """
                {"n":1e2}""", """
                {"n":12.5}""", """
                {"a":[0,9,10,99,100,1234567890,-5,-99]}""", """
                {"a":[1.5,12.5]}""",
                // Integer digit-count boundary at 19 (long vs. BigInteger fallback): exactly 19
                // digits fitting a signed long (both sign boundaries), 19 digits overflowing it
                // (both sign boundaries), and 20+ digits (always BigInteger), and the same
                // values as array elements.
                """
                {"n":9223372036854775807}""", """
                {"n":-9223372036854775808}""", """
                {"n":9223372036854775808}""", """
                {"n":-9223372036854775809}""", """
                {"n":99999999999999999999}""", """
                {"a":[9223372036854775807,-9223372036854775808,9223372036854775808,-9223372036854775809,99999999999999999999]}""",
                // DoubleParser code paths (see that class's own comments): fast path,
                // Eisel-Lemire (main, round-to-even, underflow, overflow, subnormal), and the
                // slow path (>19 significant digits).
                """
                {"d":1e22}""", """
                {"d":1e23}""", """
                {"d":9007199254740993e0}""", """
                {"d":1e-400}""", """
                {"d":-1e400}""", """
                {"d":5e-324}""", """
                {"d":2.2250738585072013e-308}""", """
                {"d":100000000000000000000.000000}""",
                // String shapes: every standard single-char escape, \\u-escaped and raw
                // (unescaped) multi-byte UTF-8, empty strings, an escape that isn't the last
                // thing in the buffer, and string elements in arrays / objects nested in arrays.
                """
                {"tab":"a\\tb"}""", """
                {"cr":"a\\rb"}""", """
                {"bs":"a\\bb"}""", """
                {"ff":"a\\fb"}""", """
                {"fslash":"a\\/b"}""", """
                {"bslash":"a\\\\b"}""", """
                {"u2byte":"\\u00E9"}""", """
                {"u3byte":"\\u4E16"}""", """
                {"surrogate":"\\uD83D\\uDE00"}""", """
                {"raw2byte":"café"}""", """
                {"raw3byte":"世界"}""", """
                {"rawEmoji":"😀"}""", """
                {"empty":""}""", """
                {"first":"x\\ny","second":2}""", """
                {"strs":["a","bb","ccc"]}""", """
                {"strsEsc":["a\\nb","c\\td","plain"]}""", """
                {"objInArr":[{"a":"x\\ny","b":"z"}]}""", """
                {"objInArrEscName":[{"x\\"y":"a\\nb"}]}"""
        );
        // end::noformat
        for (int nameLen = 1; nameLen <= 20; nameLen++) {
            docs.add("{\"" + "x".repeat(nameLen) + "\":1}");
        }

        // Long strings (200+ bytes) for real vectorization coverage.
        docs.add("{\"pre\":1,\"long\":\"" + "a".repeat(200) + "\",\"post\":2}");
        docs.add("{\"longEsc\":\"" + "a".repeat(150) + "\\n" + "a".repeat(50) + "\"}");
        docs.add("{\"longRaw\":\"" + "a".repeat(150) + "café" + "a".repeat(50) + "\"}");
        docs.add("{\"arrLong\":[\"" + "a".repeat(200) + "\",\"" + "b".repeat(100) + "\\t" + "c".repeat(80) + "\"]}");
        docs.add("{\"objArrLong\":[{\"a\":\"" + "a".repeat(200) + "\",\"b\":\"" + "b".repeat(120) + "\\n" + "b".repeat(60) + "\"}]}");
        return List.copyOf(docs);
    }
}
