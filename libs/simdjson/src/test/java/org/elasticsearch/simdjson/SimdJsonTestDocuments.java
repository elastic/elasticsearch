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
                {"first":1,"abcdefghi":2}"""
        );
        // end::noformat
        for (int nameLen = 1; nameLen <= 20; nameLen++) {
            docs.add("{\"" + "x".repeat(nameLen) + "\":1}");
        }
        return List.copyOf(docs);
    }
}
