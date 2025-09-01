/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.io.stream.StreamOutput.bytesInString;
import static org.elasticsearch.common.io.stream.StreamOutput.bytesInStringCollection;
import static org.elasticsearch.common.io.stream.StreamOutput.bytesInVInt;

public class StreamOutputTests extends ESTestCase {

    public void testBytesInVInt() {
        // 0..127 should use the single byte shortcut
        assertEquals(1, bytesInVInt(0));
        assertEquals(1, bytesInVInt(127));
        assertEquals(1, bytesInVInt(randomIntBetween(0, 127)));

        assertEquals(2, bytesInVInt(128));
        assertEquals(2, bytesInVInt(16383));
        assertEquals(2, bytesInVInt(randomIntBetween(128, 16383)));

        assertEquals(3, bytesInVInt(16384));
        assertEquals(3, bytesInVInt(2097151));
        assertEquals(3, bytesInVInt(randomIntBetween(16384, 2097151)));

        assertEquals(4, bytesInVInt(2097152));
        assertEquals(4, bytesInVInt(268435455));
        assertEquals(4, bytesInVInt(randomIntBetween(2097152, 268435455)));

        assertEquals(5, bytesInVInt(268435456));
        assertEquals(5, bytesInVInt(Integer.MAX_VALUE));
        assertEquals(5, bytesInVInt(randomIntBetween(268435456, Integer.MAX_VALUE)));
    }

    public void testBytesInString() {
        assertEquals(1, bytesInString(""));

        // Since the length of the string is stored as a VInt, this has additional bytes
        int randomSize = randomIntBetween(0, 256);
        int vintBytes = randomSize <= 127 ? 1 : 2;
        assertEquals(randomSize + vintBytes, bytesInString(randomAlphaOfLength(randomSize)));

        // This is U+00E9, and 2 bytes in UTF-8
        String s = "é";
        assertEquals(2 + 1, bytesInString(s));

        // A mixed string of 1, 2 and 3 bytes respectively
        s = "aéअ";
        assertEquals(6 + 1, bytesInString(s));
    }

    public void testBytesInStringCollection() {
        assertEquals(1, bytesInStringCollection(Collections.emptyList()));

        List<String> listOfStrings = new ArrayList<>();
        int randomSize = randomIntBetween(0, 256);
        // Start with the number of bytes needed to store the size of the collection
        int totalStringBytes = randomSize <= 127 ? 1 : 2;
        for (int i = 0; i < randomSize; i++) {
            int lengthOfString = randomIntBetween(0, 256);
            // The size of the string is stored as a VInt
            int stringBytes = lengthOfString <= 127 ? 1 : 2;
            StringBuilder s = new StringBuilder();
            for (int j = 0; j < lengthOfString; j++) {
                int characterBytes = randomIntBetween(1, 3);
                if (characterBytes == 1) {
                    s.append(randomAlphaOfLength(1));
                } else if (characterBytes == 2) {
                    s.append("é");
                } else {
                    s.append("अ");
                }
                stringBytes += characterBytes;
            }

            listOfStrings.add(s.toString());
            totalStringBytes += stringBytes;
        }

        assertEquals(totalStringBytes, bytesInStringCollection(listOfStrings));
    }
}
