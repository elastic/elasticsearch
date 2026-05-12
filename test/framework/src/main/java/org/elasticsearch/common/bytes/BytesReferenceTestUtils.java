/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Objects;

public enum BytesReferenceTestUtils {
    ;

    /**
     * Like {@link Matchers#equalTo} except it reports the contents of the respective {@link BytesReference} instances on mismatch.
     */
    public static Matcher<BytesReference> equalBytes(BytesReference expected) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return Objects.equals(expected, actual);
            }

            @Override
            public void describeTo(Description description) {
                if (expected == null) {
                    description.appendValue(null);
                } else {
                    appendBytesReferenceDescription(expected, description);
                }
            }

            private void appendBytesReferenceDescription(BytesReference bytesReference, Description description) {
                final var stringBuilder = new StringBuilder("BytesReference[");
                final var iterator = bytesReference.iterator();
                BytesRef bytesRef;
                boolean first = true;
                try {
                    while ((bytesRef = iterator.next()) != null) {
                        for (int i = 0; i < bytesRef.length; i++) {
                            if (first) {
                                first = false;
                            } else {
                                stringBuilder.append(' ');
                            }
                            stringBuilder.append(Strings.format("%02x", bytesRef.bytes[bytesRef.offset + i]));
                        }
                    }
                    description.appendText(stringBuilder.append(']').toString());
                } catch (IOException e) {
                    throw new AssertionError("no IO happens here", e);
                }
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ");
                if (item instanceof BytesReference bytesReference) {
                    appendBytesReferenceDescription(bytesReference, description);
                } else {
                    description.appendValue(item);
                }
            }
        };
    }
}
