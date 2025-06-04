/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.unit;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A unit class that encapsulates all in-exact search
 * parsing and conversion from similarities to edit distances
 * etc.
 */
public final class Fuzziness implements ToXContentFragment, Writeable {

    static final String X_FIELD_NAME = "fuzziness";
    public static final ParseField FIELD = new ParseField(X_FIELD_NAME);

    public static final Fuzziness ZERO = new Fuzziness("0");
    public static final Fuzziness ONE = new Fuzziness("1");
    public static final Fuzziness TWO = new Fuzziness("2");
    public static final Fuzziness AUTO = new Fuzziness("AUTO");

    static final int DEFAULT_LOW_DISTANCE = 3;
    static final int DEFAULT_HIGH_DISTANCE = 6;

    private final String fuzziness;
    private int lowDistance = DEFAULT_LOW_DISTANCE;
    private int highDistance = DEFAULT_HIGH_DISTANCE;

    private Fuzziness(String fuzziness) {
        this.fuzziness = fuzziness;
    }

    /**
     * Creates a {@link Fuzziness} instance from an edit distance. The value must be one of {@code [0, 1, 2]}
     * Note: Using this method only makes sense if the field you are applying Fuzziness to is some sort of string.
     * @throws IllegalArgumentException if the edit distance is not in [0, 1, 2]
     */
    public static Fuzziness fromEdits(int edits) {
        return switch (edits) {
            case 0 -> Fuzziness.ZERO;
            case 1 -> Fuzziness.ONE;
            case 2 -> Fuzziness.TWO;
            default -> throw new IllegalArgumentException("Valid edit distances are [0, 1, 2] but was [" + edits + "]");
        };
    }

    /**
     * Creates a {@link Fuzziness} instance from a String representation. This can
     * either be an edit distance where the value must be one of
     * {@code ["0", "1", "2"]} or "AUTO" for a fuzziness that depends on the term
     * length. Using the "AUTO" fuzziness, you can optionally supply low and high
     * distance arguments in the format {@code "AUTO:[low],[high]"}. See the query
     * DSL documentation for more information about how these values affect the
     * fuzziness value.
     * Note: Using this method only makes sense if the field you
     * are applying Fuzziness to is some sort of string.
     */
    public static Fuzziness fromString(String fuzzinessString) {
        if (Strings.isEmpty(fuzzinessString)) {
            throw new IllegalArgumentException("fuzziness cannot be null or empty.");
        }
        String upperCase = fuzzinessString.toUpperCase(Locale.ROOT);
        // check if it is one of the "AUTO" variants
        if (upperCase.equals("AUTO")) {
            return Fuzziness.AUTO;
        } else if (upperCase.startsWith("AUTO:")) {
            return parseCustomAuto(upperCase);
        } else {
            // should be a float or int representing a valid edit distance, otherwise throw error
            try {
                float parsedFloat = Float.parseFloat(upperCase);
                if (parsedFloat % 1 > 0) {
                    throw new IllegalArgumentException("fuzziness needs to be one of 0.0, 1.0 or 2.0 but was " + parsedFloat);
                }
                return fromEdits((int) parsedFloat);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("fuzziness cannot be [" + fuzzinessString + "].", e);
            }
        }
    }

    /**
     * Read from a stream.
     */
    public Fuzziness(StreamInput in) throws IOException {
        fuzziness = in.readString();
        if (in.readBoolean()) {
            lowDistance = in.readVInt();
            highDistance = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fuzziness);
        // we cannot serialize the low/high bounds since the other node does not know about them.
        // This is a best-effort to not fail queries in case the cluster is being upgraded and users
        // start using features that are not available on all nodes.
        if (isAutoWithCustomValues()) {
            out.writeBoolean(true);
            out.writeVInt(lowDistance);
            out.writeVInt(highDistance);
        } else {
            out.writeBoolean(false);
        }
    }

    private static Fuzziness parseCustomAuto(final String fuzzinessString) {
        assert fuzzinessString.toUpperCase(Locale.ROOT).startsWith(AUTO.asString() + ":");
        String[] fuzzinessLimit = fuzzinessString.substring(AUTO.asString().length() + 1).split(",");
        if (fuzzinessLimit.length == 2) {
            try {
                int lowerLimit = Integer.parseInt(fuzzinessLimit[0]);
                int highLimit = Integer.parseInt(fuzzinessLimit[1]);
                if (lowerLimit < 0 || highLimit < 0 || lowerLimit > highLimit) {
                    throw new ElasticsearchParseException(
                        "fuzziness wrongly configured [{}]. Must be 0 < lower value <= higher value.",
                        fuzzinessString
                    );
                }
                Fuzziness fuzziness = new Fuzziness("AUTO");
                fuzziness.lowDistance = lowerLimit;
                fuzziness.highDistance = highLimit;
                return fuzziness;
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("failed to parse [{}] as a \"auto:int,int\"", e, fuzzinessString);
            }
        } else {
            throw new ElasticsearchParseException("failed to find low and high distance values");
        }
    }

    public static Fuzziness parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        return switch (token) {
            case VALUE_STRING -> fromString(parser.text());
            case VALUE_NUMBER -> fromEdits(parser.intValue());
            default -> throw new IllegalArgumentException("Can't parse fuzziness on token: [" + token + "]");
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(X_FIELD_NAME, asString());
        return builder;
    }

    public int asDistance() {
        return asDistance(null);
    }

    public int asDistance(String text) {
        if (this.equals(AUTO) || isAutoWithCustomValues()) { // AUTO
            final int len = termLen(text);
            if (len < lowDistance) {
                return 0;
            } else if (len < highDistance) {
                return 1;
            } else {
                return 2;
            }
        }
        return Math.min(2, (int) asFloat());
    }

    public float asFloat() {
        if (this.equals(AUTO) || isAutoWithCustomValues()) {
            return 1f;
        }
        return Float.parseFloat(fuzziness);
    }

    private static int termLen(String text) {
        return text == null ? 5 : text.codePointCount(0, text.length()); // 5 avg term length in english
    }

    public String asString() {
        if (isAutoWithCustomValues()) {
            return fuzziness + ":" + lowDistance + "," + highDistance;
        }
        return fuzziness;
    }

    private boolean isAutoWithCustomValues() {
        return fuzziness.equals("AUTO") && (lowDistance != DEFAULT_LOW_DISTANCE || highDistance != DEFAULT_HIGH_DISTANCE);
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
        return Objects.equals(fuzziness, other.fuzziness) && lowDistance == other.lowDistance && highDistance == other.highDistance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fuzziness, lowDistance, highDistance);
    }
}
