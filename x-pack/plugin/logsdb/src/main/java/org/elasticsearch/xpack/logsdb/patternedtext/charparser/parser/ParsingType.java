/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.Type;

import java.util.Locale;

public abstract class ParsingType implements Type {

    protected final String name;
    protected final EncodingType encodingType;

    /**
     * The number of sub-tokens that this parsing type is composed of.
     * For example, a token type that represents a date would typically be composed of 3 sub-tokens: year, month, and day.
     * A multi-token type that represents a full timestamp would typically be composed of at least 6 timestamp components: year, month, day,
     * hour, minute, and second.
     * Sub-token types will always have numSubTokens = 1.
     */
    private final int numSubTokens;

    protected final TimestampFormat timestampFormat;

    /**
     * Higher-level entity bitmask for this parsing type by position.
     * For example, if this parsing type is a sub-token, the bitmask at position 2 will indicate
     * the token types in which this sub-token can be found at the third position.
     */
    protected final int[] higherLevelBitmaskByPosition;

    protected ParsingType(
        String name,
        EncodingType encodingType,
        int numSubTokens,
        TimestampFormat timestampFormat,
        int[] higherLevelBitmaskByPosition
    ) {
        if (timestampFormat == null) {
            if (encodingType == EncodingType.TIMESTAMP) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Multi-token type %s is a timestamp, but does not have a timestamp format defined", name)
                );
            }
        }
        this.name = name;
        this.encodingType = encodingType;
        this.numSubTokens = numSubTokens;
        this.timestampFormat = timestampFormat;
        this.higherLevelBitmaskByPosition = higherLevelBitmaskByPosition;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public EncodingType encodingType() {
        return encodingType;
    }

    public int getNumSubTokens() {
        return numSubTokens;
    }

    public TimestampFormat getTimestampFormat() {
        return timestampFormat;
    }

    /**
     * Returns the higher-level entity bitmask for the specified position of this parsing type within its higher-level entity.
     * For example, if this parsing type is a sub-token, the bitmask returned when invoking this method with position 2 will indicate
     * the token types in which this sub-token can be found at the third position.
     *
     * @param position the position in the token
     * @return the bitmask for the specified position
     */
    public int getHigherLevelBitmaskByPosition(int position) {
        if (higherLevelBitmaskByPosition == null) {
            throw new UnsupportedOperationException("This parsing type does not support higher-level bitmask by position");
        }
        return higherLevelBitmaskByPosition[position];
    }
}
