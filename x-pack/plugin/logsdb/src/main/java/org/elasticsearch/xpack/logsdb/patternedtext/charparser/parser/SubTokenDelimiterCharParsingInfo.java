/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

/**
 * An immutable class that holds information about a subToken delimiter character.
 */
public final class SubTokenDelimiterCharParsingInfo {

    /**
     * The subToken delimiter character that this processor is responsible for.
     */
    public final char character;

    /**
     * Indicates what token bitmask is valid for this character at each position between two subTokens.
     * For example, the bitmask at index 2 indicates what tokens may be relevant if the current delimiter character is found between
     * the third and fourth subTokens of the currently parsed token.
     */
    public final int[] tokenBitmaskPerSubTokenIndex;

    /**
     * A fast-access lookup table for finding the token bitmask generator based on the subToken index.
     * This array contains instances of {@link SubTokenEvaluator} for each subToken index, specific to the delimiter character represented
     * by the current instance.
     * The index in the array corresponds to the index of the subToken within the parsed token.
     * For example, the generator at index 2 is used to generate the bitmask for the third subToken of the currently parsed token.
     * This array is relevant only for subTokens of type string.
     */
    public final SubTokenEvaluator<SubstringView>[] subTokenEvaluatorPerSubTokenIndices;

    public SubTokenDelimiterCharParsingInfo(
        char character,
        int[] tokenBitmaskPerSubTokenIndex,
        SubTokenEvaluator<SubstringView>[] subTokenEvaluatorPerSubTokenIndices
    ) {
        this.character = character;
        this.tokenBitmaskPerSubTokenIndex = tokenBitmaskPerSubTokenIndex;
        this.subTokenEvaluatorPerSubTokenIndices = subTokenEvaluatorPerSubTokenIndices;
    }
}
