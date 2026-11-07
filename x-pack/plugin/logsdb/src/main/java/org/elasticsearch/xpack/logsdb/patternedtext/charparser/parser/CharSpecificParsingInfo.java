/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.function.ToIntFunction;

/**
 * An immutable class that holds information about a specific character-related parsing details (e.g., a subToken delimiter character).
 */
public final class CharSpecificParsingInfo {

    /**
     * The that this info instance is responsible for.
     */
    public final char character;

    /**
     * This array indicates what token bitmask is valid for the delimiter character represented by the current instance at each position
     * between two subTokens of the parsed token. For example, the bitmask at index 2 indicates what tokens
     * are valid if the current delimiter character is found between the third and fourth subTokens of the currently parsed token.
     */
    public final int[] tokenBitmaskPerDelimiterPosition;

    /**
     * Provides a way to generate a sub-token bitmask based on a substring for each location within a token.
     * This array contains instances of {@link ToIntFunction} for each subToken index, specific to the delimiter represented by the
     * current instance. The index in the array corresponds to the index of the sub-token within the parsed token.
     * For example, the generator function at index 2 is used to generate the bitmask of all valid sub-tokens that are the third sub-token
     * in the parsed token, based on the substring found between the second and third occurrence of the delimiter character.
     */
    public final ToIntFunction<SubstringView>[] bitmaskGeneratorPerPosition;

    /**
     * This array indicates what multi-token bitmask is valid when the character represented by the current instance is found at each
     * position within the total length of multi-token delimiter parts.
     * For example, given the following multi-token format: "$Mon, $DD $YYYY $timeS $AP", the full concatenated string made up of the
     * delimiter parts is ",    ". Therefore, the bit of this multi-token will be set at index 0 for the character ',' and at indices 1,
     * 2, 3, and 4 for the space character. This enables exact match of multi-token formats.
     */
    public final int[] multiTokenBitmaskPerDelimiterPartPosition;

    public CharSpecificParsingInfo(
        char character,
        int[] tokenBitmaskPerDelimiterPosition,
        ToIntFunction<SubstringView>[] bitmaskGeneratorPerPosition,
        int[] multiTokenBitmaskPerDelimiterPartPosition
    ) {
        this.character = character;
        this.tokenBitmaskPerDelimiterPosition = tokenBitmaskPerDelimiterPosition;
        this.bitmaskGeneratorPerPosition = bitmaskGeneratorPerPosition;
        this.multiTokenBitmaskPerDelimiterPartPosition = multiTokenBitmaskPerDelimiterPartPosition;
    }
}
