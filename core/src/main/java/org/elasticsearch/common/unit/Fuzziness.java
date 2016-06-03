/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.common.unit;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A unit class that encapsulates all in-exact search
 * parsing and conversion from similarities to edit distances
 * etc.
 */
public final class Fuzziness implements ToXContent, Writeable {

    public static final String X_FIELD_NAME = "fuzziness";
    public static final Fuzziness ZERO = new Fuzziness(0);
    public static final Fuzziness ONE = new Fuzziness(1);
    public static final Fuzziness TWO = new Fuzziness(2);
    public static final Fuzziness AUTO = new Fuzziness("AUTO");
    public static final ParseField FIELD = new ParseField(X_FIELD_NAME);

    private final String fuzziness;

    private Fuzziness(int fuzziness) {
        if (fuzziness != 0 && fuzziness != 1 && fuzziness != 2) {
            throw new IllegalArgumentException("Valid edit distances are [0, 1, 2] but was [" + fuzziness + "]");
        }
        this.fuzziness = Integer.toString(fuzziness);
    }

    private Fuzziness(String fuzziness) {
        if (fuzziness == null) {
            throw new IllegalArgumentException("fuzziness can't be null!");
        }
        this.fuzziness = fuzziness.toUpperCase(Locale.ROOT);
    }

    /**
     * Read from a stream.
     */
    public Fuzziness(StreamInput in) throws IOException {
        fuzziness = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fuzziness);
    }

    /**
     * Creates a {@link Fuzziness} instance from an edit distance. The value must be one of <tt>[0, 1, 2]</tt>
     *
     * Note: Using this method only makes sense if the field you are applying Fuzziness to is some sort of string.
     */
    public static Fuzziness fromEdits(int edits) {
        return new Fuzziness(edits);
    }

    public static Fuzziness build(Object fuzziness) {
        if (fuzziness instanceof Fuzziness) {
            return (Fuzziness) fuzziness;
        }
        String string = fuzziness.toString();
        if (AUTO.asString().equalsIgnoreCase(string)) {
            return AUTO;
        }
        return new Fuzziness(string);
    }

    public static Fuzziness parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        switch (token) {
            case VALUE_STRING:
            case VALUE_NUMBER:
                final String fuzziness = parser.text();
                if (AUTO.asString().equalsIgnoreCase(fuzziness)) {
                    return AUTO;
                }
                try {
                    final int minimumSimilarity = Integer.parseInt(fuzziness);
                    switch (minimumSimilarity) {
                        case 0:
                            return ZERO;
                        case 1:
                            return ONE;
                        case 2:
                            return TWO;
                        default:
                            return build(fuzziness);
                    }
                } catch (NumberFormatException ex) {
                    return build(fuzziness);
                }

            default:
                throw new IllegalArgumentException("Can't parse fuzziness on token: [" + token + "]");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, true);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean includeFieldName) throws IOException {
        if (includeFieldName) {
            builder.field(X_FIELD_NAME, fuzziness);
        } else {
            builder.value(fuzziness);
        }
        return builder;
    }

    public int asDistance() {
        return asDistance(null);
    }

    public int asDistance(String text) {
        if (this.equals(AUTO)) { //AUTO
            final int len = termLen(text);
            if (len <= 2) {
                return 0;
            } else if (len > 5) {
                return 2;
            } else {
                return 1;
            }
        }
        return Math.min(2, (int) asFloat());
    }

    public float asFloat() {
        if (this.equals(AUTO)) {
            return 1f;
        }
        return Float.parseFloat(fuzziness.toString());
    }

    private int termLen(String text) {
        return text == null ? 5 : text.codePointCount(0, text.length()); // 5 avg term length in english
    }

    public String asString() {
        return fuzziness.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Fuzziness other = (Fuzziness) obj;
        return Objects.equals(fuzziness, other.fuzziness);
    }

    @Override
    public int hashCode() {
        return fuzziness.hashCode();
    }
}
