/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.BitmaskRegistry;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.CharSpecificParsingInfo;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.MultiTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringToIntegerMap;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.Schema;

/**
 * Holds the compiled form of the schema.yaml file contents for the parser. See {@link Schema} for more details.
 * The compiled form is essentially a set of fast-access lookup tables that allow the parser to quickly update the parsing state of the
 * currently parsed subToken, token, and multi-token. States are represented through bitmasks. During schema compilation, each subToken,
 * token, and multi-token is assigned a unique bit in a bitmask. During parsing, the subToken bitmask represents a superset of all
 * subToken bits that correspond subToken-types that are still valid for the current subToken being parsed (meaning - all such that
 * have not been eliminated yet). Similarly, the current token bitmask represents all valid tokens for the current token being parsed,
 * and the current multi-token bitmask represents all valid multi-tokens for the recent tokens.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class CompiledSchema {
    /**
     * A fast-access lookup table for finding the subToken bitmask based on the character.
     * This array contains bitmask mappings for only and all ASCII characters.
     */
    public final int[] charToSubTokenBitmask;

    /**
     * A fast-access lookup table for finding the character type based on the character.
     * This array contains type mappings for only and all ASCII characters.
     */
    public final byte[] charToCharType;

    /**
     * A fast-access lookup table for finding parsing information for delimiter characters.
     * This array contains instances of {@link CharSpecificParsingInfo} for delimiter characters, where the index in the array
     * corresponds to the ASCII code of the character.
     * This means that the array is sparse and only contains information for characters that are defined as delimiters in the schema.
     */
    public final CharSpecificParsingInfo[] charSpecificParsingInfos;

    /**
     * A fast-access map for retrieving the numeric value representation for String subTokens.
     */
    public final SubstringToIntegerMap subTokenNumericValueRepresentation;

    /**
     * The maximum number of subTokens that can be parsed from a single token.
     */
    public final int maxSubTokensPerToken;

    /**
     * The maximum number of tokens that can be parsed from a single multi-token.
     */
    public final int maxTokensPerMultiToken;

    /**
     * A bitmask with only the generic integer subToken bit set.
     */
    public final int intSubTokenBitmask;

    /**
     * A bitmask that represents all subTokens types that are of an integer type.
     */
    public final int allIntegerSubTokenBitmask;

    /**
     * A bitmask that represents all generic subToken types, such as integer, hexadecimal, keyword, etc.
     */
    public final int genericSubTokenTypesBitmask;

    /**
     * A fast-access bitmask lookup table for integer subTokens. This requires a two-step lookup.
     * See {@link #integerSubTokenBitmaskArrayRanges} for details.
     */
    public final int[] integerSubTokenBitmasks;

    /**
     * An auxiliary array that is used to determine the proper bitmask index within the {@link #integerSubTokenBitmasks} array for each
     * integer value.
     * Entries in this array contain the upper bounds (inclusive) of ranges of integers that share the same bitmask. The lookup in this
     * array is done using binary search. Once the upper bound is found, its index within this array indicates the proper bitmask index
     * within the {@link #integerSubTokenBitmasks} array. The last entry in this array is always {@link Integer#MAX_VALUE}.
     * <p>
     * For example, consider this array: [0, 10, {@link Integer#MAX_VALUE}]. The first entry (0) indicates that the first bitmask in
     * {@link #integerSubTokenBitmasks} is valid for all integers from {@link Integer#MIN_VALUE} to 0 (inclusive).
     * The second entry (10) indicates that the second bitmask in {@link #integerSubTokenBitmasks} is valid for all integers from 1 to 10
     * (inclusive). The last entry (Integer.MAX_VALUE) indicates that the third bitmask in {@link #integerSubTokenBitmasks} is valid for all
     * integers from 11 to {@link Integer#MAX_VALUE} (inclusive).
     * So, when looking for the matching bitmask for the integer value of 5, we would determine that we need to use the second bitmask from
     * the {@link #integerSubTokenBitmasks} array.
     * </p>
     */
    public final int[] integerSubTokenBitmaskArrayRanges;

    /**
     * A fast-access bitmask lookup table for small integers. This provides an optimization for small integers, that are more common,
     * over the two-step lookup (and a binary search) that is required for using {@link #integerSubTokenBitmasks}.
     */
    public final int[] smallIntegerSubTokenBitmasks;

    /**
     * A fast-access token bitmask lookup table for each number of subTokens. For example, the bitmask at index 3 indicates the token
     * types that have 4 subTokens.
     */
    public final int[] subTokenCountToTokenBitmask;

    /**
     * A fast-access multi-token bitmask lookup table for each number of tokens. For example, the bitmask at index 2 indicates the
     * multi-token types that have 3 tokens.
     */
    public final int[] tokenCountToMultiTokenBitmask;

    /**
     * A fast-access multi-token bitmask lookup table for each number of subTokens. For example, the bitmask at index 5 indicates the
     * multi-token types that have 6 subTokens (regardless of the number of tokens).
     */
    public final int[] subTokenCountToMultiTokenBitmask;

    /**
     * A fast-access multi-token bitmask lookup table for each total length of delimiter parts. For example, the bitmask at index 10
     * indicates the multi-token types that have delimiter parts with a total length of 10 characters. Delimiter parts are the literal
     * strings between tokens in a multi-token format.
     */
    public final int[] delimiterPartsTotalLengthToMultiTokenBitmask;

    /**
     * A subToken bitmask registry that allows for fast access to subToken types by their bit index or bitmask.
     */
    public final BitmaskRegistry<SubTokenType> subTokenBitmaskRegistry;

    /**
     * A token bitmask registry that allows for fast access to token types by their bit index or bitmask.
     */
    public final BitmaskRegistry<TokenType> tokenBitmaskRegistry;

    /**
     * The maximum number of subTokens that can be parsed from a single multi-token.
     */
    public final int maxSubTokensPerMultiToken;

    /**
     * A multi-token bitmask registry that allows for fast access to multi-token types by their bit index or bitmask.
     */
    public final BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry;

    public CompiledSchema(
        int[] charToSubTokenBitmask,
        byte[] charToCharType,
        CharSpecificParsingInfo[] charSpecificParsingInfos,
        SubstringToIntegerMap subTokenNumericValueRepresentation,
        int maxSubTokensPerToken,
        int maxTokensPerMultiToken,
        int maxSubTokensPerMultiToken,
        int intSubTokenBitmask,
        int allIntegerSubTokenBitmask,
        int genericSubTokenTypesBitmask,
        int[] integerSubTokenBitmasks,
        int[] integerSubTokenBitmaskArrayRanges,
        int[] smallIntegerSubTokenBitmasks,
        int[] subTokenCountToTokenBitmask,
        int[] tokenCountToMultiTokenBitmask,
        int[] subTokenCountToMultiTokenBitmask,
        int[] delimiterPartsTotalLengthToMultiTokenBitmask,
        BitmaskRegistry<SubTokenType> subTokenBitmaskRegistry,
        BitmaskRegistry<TokenType> tokenBitmaskRegistry,
        BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry
    ) {
        this.charToSubTokenBitmask = charToSubTokenBitmask;
        this.charToCharType = charToCharType;
        this.charSpecificParsingInfos = charSpecificParsingInfos;
        this.subTokenNumericValueRepresentation = subTokenNumericValueRepresentation;
        this.maxSubTokensPerToken = maxSubTokensPerToken;
        this.maxTokensPerMultiToken = maxTokensPerMultiToken;
        this.maxSubTokensPerMultiToken = maxSubTokensPerMultiToken;
        this.intSubTokenBitmask = intSubTokenBitmask;
        this.allIntegerSubTokenBitmask = allIntegerSubTokenBitmask;
        this.genericSubTokenTypesBitmask = genericSubTokenTypesBitmask;
        this.integerSubTokenBitmasks = integerSubTokenBitmasks;
        this.integerSubTokenBitmaskArrayRanges = integerSubTokenBitmaskArrayRanges;
        this.smallIntegerSubTokenBitmasks = smallIntegerSubTokenBitmasks;
        this.subTokenCountToTokenBitmask = subTokenCountToTokenBitmask;
        this.tokenCountToMultiTokenBitmask = tokenCountToMultiTokenBitmask;
        this.subTokenCountToMultiTokenBitmask = subTokenCountToMultiTokenBitmask;
        this.delimiterPartsTotalLengthToMultiTokenBitmask = delimiterPartsTotalLengthToMultiTokenBitmask;
        if (subTokenBitmaskRegistry.isSealed() == false) {
            throw new IllegalArgumentException("SubToken bitmask registry must be sealed before passing to the compiled schema");
        }
        this.subTokenBitmaskRegistry = subTokenBitmaskRegistry;
        if (tokenBitmaskRegistry.isSealed() == false) {
            throw new IllegalArgumentException("Token bitmask registry must be sealed before passing to the compiled schema");
        }
        this.tokenBitmaskRegistry = tokenBitmaskRegistry;
        if (multiTokenBitmaskRegistry.isSealed() == false) {
            throw new IllegalArgumentException("Multi-token bitmask registry must be sealed before passing to the compiled schema");
        }
        this.multiTokenBitmaskRegistry = multiTokenBitmaskRegistry;
    }
}
