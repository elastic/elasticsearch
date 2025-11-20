/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.DoubleArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.HexadecimalArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IPv4Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IntegerArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.KeywordArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParseException;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Parser;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Sign;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Timestamp;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.UUIDArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.CompiledSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.ToIntFunction;

import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.DIGIT_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.LINE_END_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_BOUNDARY_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_DELIMITER_CHAR_CODE;

/**
 * During parsing, the different current bitmasks represent a superset of all applicable types for the currently parsed entity (subToken,
 * token, or multi-token).
 * Entity types are excluded through elimination, meaning that the bitmasks are reset to contain all possible entity types at the start of
 * each entity parsing, and then are updated through AND operations.
 */
@SuppressWarnings("ExtractMethodRecommender")
public final class CharParser implements Parser {

    // this is the compiled schema information
    private final CompiledSchema compiledSchema;

    private final BitmaskRegistry<SubTokenType> subTokenBitmaskRegistry;
    private final BitmaskRegistry<TokenType> tokenBitmaskRegistry;
    private final BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry;

    // a fast lookup table for subToken bitmasks based on the character
    private final int[] charToSubTokenBitmask;
    private final int numUsedCharacters;

    // a fast lookup table for character types based on the character
    private final byte[] charToCharType;

    private final CharSpecificParsingInfo[] charSpecificParsingInfos;
    private final SubstringToIntegerMap subTokenNumericValueRepresentationMap;

    // a fast lookup table for all valid multi-token bitmasks based on the length of the concatenated delimiter parts
    private final int[] delimiterPartsLengthToMultiTokenBitmask;

    // special bitmasks
    private final int intSubTokenBitmask;
    private final int genericSubTokenTypesBitmask;
    private final int allSubTokenBitmask;
    private final int allTokenBitmask;
    private final int allMultiTokenBitmask;
    private final int maxSubTokensPerMultiToken;
    private final int maxTokensPerMultiToken;
    private final int smallIntegerSubTokenUpperBound;

    // current subToken state
    private int currentSubTokenStartIndex;
    private int currentSubTokenBitmask;
    private int currentSubTokenIntValue;
    private boolean isCurSubTokenContainsDigits;
    private int currentSubTokenPrefixEndIndex;
    private int currentSubTokenSuffixStartIndex;
    private Sign currentSubTokenSignPrefix;

    // current token state
    private int currentTokenBitmask;
    private int currentTokenStartIndex;
    private int currentTokenSubTokenStartIndex;
    private int currentTokenSubTokenIndex;
    private boolean isPotentialDecimalNumber;

    // current multi-token state
    private int currentMultiTokenStartIndex;
    private int currentMultiTokenBitmask;
    private int currentDelimiterPartPosition;

    // sub-token buffers
    private int bufferedSubTokensIndex;
    private final int[] bufferedSubTokenBitmasks;
    private final int[] bufferedSubTokenIntValues;
    private final Sign[] bufferedSubTokenSigns;
    private final int[] bufferedSubTokenStartIndexes;
    private final int[] bufferedSubTokenLengths;

    // token buffers
    private int bufferedTokensIndex;
    private final TokenType[] bufferedTokens;
    private final int[] bufferedTokenStartIndexes;
    private final int[] bufferedTokenLengths;
    private final boolean[] isBufferedTokenDecimalNumber;
    // the index of the first sub-token and number of sub-tokens for each buffered token
    private final int[] bufferedTokenSubTokenFirstIndexes;
    private final int[] bufferedTokenSubTokenLastIndexes;

    public CharParser(CompiledSchema compiledSchema) {
        this.compiledSchema = compiledSchema;
        this.subTokenBitmaskRegistry = compiledSchema.subTokenBitmaskRegistry;
        this.tokenBitmaskRegistry = compiledSchema.tokenBitmaskRegistry;
        this.multiTokenBitmaskRegistry = compiledSchema.multiTokenBitmaskRegistry;
        this.charToSubTokenBitmask = compiledSchema.charToSubTokenBitmask;
        this.numUsedCharacters = this.charToSubTokenBitmask.length;
        this.charToCharType = compiledSchema.charToCharType;
        this.charSpecificParsingInfos = compiledSchema.charSpecificParsingInfos;
        this.subTokenNumericValueRepresentationMap = compiledSchema.subTokenNumericValueRepresentation;
        this.delimiterPartsLengthToMultiTokenBitmask = compiledSchema.delimiterPartsTotalLengthToMultiTokenBitmask;
        this.intSubTokenBitmask = compiledSchema.intSubTokenBitmask;
        this.genericSubTokenTypesBitmask = compiledSchema.genericSubTokenTypesBitmask;
        this.allSubTokenBitmask = subTokenBitmaskRegistry.getCombinedBitmask();
        this.allTokenBitmask = tokenBitmaskRegistry.getCombinedBitmask();
        this.allMultiTokenBitmask = multiTokenBitmaskRegistry.getCombinedBitmask();
        this.maxSubTokensPerMultiToken = compiledSchema.maxSubTokensPerMultiToken;
        this.maxTokensPerMultiToken = compiledSchema.maxTokensPerMultiToken;
        this.smallIntegerSubTokenUpperBound = compiledSchema.smallIntegerSubTokenBitmasks.length;

        bufferedSubTokenBitmasks = new int[maxSubTokensPerMultiToken];
        bufferedSubTokenIntValues = new int[maxSubTokensPerMultiToken];
        bufferedSubTokenSigns = new Sign[maxSubTokensPerMultiToken];
        bufferedSubTokenStartIndexes = new int[maxSubTokensPerMultiToken];
        bufferedSubTokenLengths = new int[maxSubTokensPerMultiToken];

        bufferedTokens = new TokenType[maxTokensPerMultiToken];
        bufferedTokenStartIndexes = new int[maxTokensPerMultiToken];
        bufferedTokenLengths = new int[maxTokensPerMultiToken];
        isBufferedTokenDecimalNumber = new boolean[maxTokensPerMultiToken];
        bufferedTokenSubTokenFirstIndexes = new int[maxTokensPerMultiToken];
        bufferedTokenSubTokenLastIndexes = new int[maxTokensPerMultiToken];
    }

    private void resetSubTokenState() {
        currentSubTokenBitmask = allSubTokenBitmask;
        currentSubTokenStartIndex = -1;
        currentSubTokenIntValue = 0;
        isCurSubTokenContainsDigits = false;
        currentSubTokenPrefixEndIndex = -1;
        currentSubTokenSuffixStartIndex = -1;
        currentSubTokenSignPrefix = null;
    }

    private void resetTokenState() {
        currentTokenBitmask = allTokenBitmask;
        currentTokenStartIndex = -1;
        currentTokenSubTokenStartIndex = -1;
        currentTokenSubTokenIndex = -1;
        isPotentialDecimalNumber = false;
    }

    private void resetMultiTokenState() {
        currentMultiTokenBitmask = allMultiTokenBitmask;
        currentMultiTokenStartIndex = -1;
        currentDelimiterPartPosition = -1;
    }

    private void resetBuffers() {
        // no need to actually reset buffers, enough to reset the indexes
        bufferedSubTokensIndex = -1;
        bufferedTokensIndex = -1;
    }

    private void reset() {
        resetSubTokenState();
        resetTokenState();
        resetMultiTokenState();
        resetBuffers();
    }

    /**
     * Parses a raw text message and extracts an ordered list of typed arguments.
     *
     * <p>The algorithm operates on three levels: sub-tokens → tokens → multi-tokens, using bitmasks to track
     * which entity types remain valid as parsing progresses. Each level eliminates invalid possibilities
     * through intersection (AND) operations on bitmasks.
     *
     * <p><strong>Overall Algorithm:</strong>
     * <ol>
     * <li>Initialize all bitmasks to include all possible entity types</li>
     * <li>Process each character, updating current sub-token state and bitmasks</li>
     * <li>On delimiter encounters, finalize entities in order: sub-token → token → multi-token</li>
     * <li>When entities become invalid (bitmask = 0), flush buffered content as pattern text or arguments</li>
     * <li>Continue until end of message, then create final PatternedMessage</li>
     * </ol>
     *
     * <p><strong>Entity Finalization Process:</strong>
     *
     * <p><em>Sub-token Finalization (on any delimiter):</em>
     * <ul>
     * <li>Validate sub-token length and content against schema constraints</li>
     * <li>For integer sub-tokens: lookup bitmask based on numeric value</li>
     * <li>For string sub-tokens: evaluate against constraint functions</li>
     * <li>Update token bitmask based on sub-token validity and position</li>
     * <li>Buffer sub-token data for potential token/multi-token creation</li>
     * </ul>
     *
     * <p><em>Token Finalization (on token/line delimiters):</em>
     * <ul>
     * <li>Validate token against sub-token count constraints</li>
     * <li>If valid: determine token type, buffer for multi-token evaluation</li>
     * <li>Update multi-token bitmask based on token type and position</li>
     * <li>Copy sub-token values to multi-token buffer for timestamp creation</li>
     * </ul>
     *
     * <p><em>Multi-token Finalization (when bitmask becomes invalid):</em>
     * <ul>
     * <li>Attempt to create multi-token from buffered tokens (e.g., timestamps)</li>
     * <li>If successful: create typed argument, add to result</li>
     * <li>If failed: process each buffered token individually as arguments</li>
     * <li>Process remaining sub-tokens as either typed arguments or literal text</li>
     * </ul>
     *
     * <p><strong>Bitmask Strategy:</strong>
     * <p>Each entity maintains a bitmask representing valid types. As parsing progresses:
     * <ul>
     * <li>Character validation: {@code bitmask &= charToSubTokenBitmask[char]}</li>
     * <li>Position validation: {@code bitmask &= validTypesForPosition[position]}</li>
     * <li>Constraint validation: {@code bitmask &= constraintEvaluationResult}</li>
     * <li>When bitmask becomes 0, the entity is invalid and triggers buffer flushing</li>
     * </ul>
     *
     * @param rawMessage the input message to parse
     * @return an ordered list of typed arguments extracted from the message
     */
    @SuppressWarnings("fallthrough")
    public List<Argument<?>> parse(String rawMessage) throws ParseException {
        if (rawMessage == null || rawMessage.isEmpty()) {
            return Collections.emptyList();
        }
        reset();
        SubstringView substringView = new SubstringView(rawMessage);
        List<Argument<?>> templateArguments = new ArrayList<>();
        for (int indexWithinRawMessage = 0; indexWithinRawMessage <= rawMessage.length(); indexWithinRawMessage++) {
            byte charType;
            char currentChar;
            if (indexWithinRawMessage == rawMessage.length()) {
                currentChar = ' ';
                charType = LINE_END_CODE;
            } else {
                currentChar = rawMessage.charAt(indexWithinRawMessage);
                charType = charToCharType[currentChar];
            }

            if (currentSubTokenStartIndex < 0) {
                currentSubTokenStartIndex = indexWithinRawMessage;
            }

            // The following check may break when dealing with non-ASCII characters, specifically for code points in the range
            // 0xD800 to 0xDFFF that are used for surrogate pairs (four bytes) in UTF-16. For now, we assume we only allow ASCII characters
            // in the schema settings.
            if (currentChar > numUsedCharacters) {
                // in the future we may want to handle non-ASCII characters, but for now we treat current tokens are simple text
                currentSubTokenBitmask = currentTokenBitmask = currentMultiTokenBitmask = 0;
            }

            currentSubTokenBitmask &= charToSubTokenBitmask[currentChar];

            switch (charType) {
                case DIGIT_CHAR_CODE:
                    isCurSubTokenContainsDigits = true;
                    currentSubTokenIntValue = currentSubTokenIntValue * 10 + currentChar - '0';
                    break;
                case SUBTOKEN_DELIMITER_CHAR_CODE:
                    if (currentChar == '-') {
                        if (currentSubTokenStartIndex == indexWithinRawMessage) {
                            currentSubTokenSignPrefix = Sign.MINUS;
                            // don't treat as a delimiter but as a sign prefix - continue parsing next character
                            break;
                        }
                    } else if (currentChar == '+') {
                        if (currentSubTokenStartIndex == indexWithinRawMessage) {
                            currentSubTokenSignPrefix = Sign.PLUS;
                            // don't treat as a delimiter but as a sign prefix - continue parsing next character
                            break;
                        }
                    } else if (currentChar == '.') {
                        isPotentialDecimalNumber = currentTokenSubTokenIndex < 0 && (currentSubTokenBitmask & intSubTokenBitmask) != 0;
                    }

                    // everything we need to do at the end of a sub-token we also must do at the end of a token, so we share the logic
                    // in the next case - fallthrough is intended
                case TOKEN_DELIMITER_CHAR_CODE:
                    // everything we need to do at the end of a token we also must do at the end of a line, so we share the logic
                    // in the next case - fallthrough is intended
                case LINE_END_CODE:
                    boolean flushBufferedInfo = false;

                    // whether we are processing a subToken delimiter or a token delimiter, once we encounter a state that invalidates
                    // the current parsed entity, we abort and write all buffered info (tokens and subTokens) to the pattern and/or
                    // as arguments in the right order: multi-token, token, subTokens.
                    // breaking from this case without writing the buffered info should happen only when we are still within a token
                    // parsing, or within a multi-token parsing.

                    // currentSubTokenEndIndex is exclusive, meaning it points to the first character after the current subToken
                    int currentSubTokenEndIndex = (currentSubTokenSuffixStartIndex >= 0)
                        ? currentSubTokenSuffixStartIndex
                        : indexWithinRawMessage;
                    int currentSubTokenLength = currentSubTokenEndIndex - currentSubTokenStartIndex;

                    if (currentSubTokenLength == 0) {
                        // empty tokens (for example, just "-") should have a zero bitmask, so we must change from the default non-zero
                        currentSubTokenBitmask = 0;
                    }

                    currentTokenSubTokenIndex++;
                    if (currentSubTokenBitmask == 0 || currentTokenSubTokenIndex == compiledSchema.maxSubTokensPerToken) {
                        // we either already passed the maximum number of subTokens for any known token, or the current subToken is
                        // invalid - both indication that the current token is invalid
                        currentTokenBitmask = 0;
                    }

                    if (currentSubTokenSignPrefix == Sign.MINUS) {
                        currentSubTokenIntValue = -currentSubTokenIntValue;
                    }

                    CharSpecificParsingInfo delimiterParsingInfo = null;
                    if (currentTokenBitmask == 0) {
                        // no need to evaluate specific subToken types, the generic type would be enough to create generic arguments
                        flushBufferedInfo = true;
                    } else {
                        delimiterParsingInfo = charSpecificParsingInfos[currentChar];

                        // update the current token bitmask to include only valid tokens with the current delimiter character in the
                        // current subToken position
                        currentTokenBitmask &= delimiterParsingInfo.tokenBitmaskPerDelimiterPosition[currentTokenSubTokenIndex];

                        // here we enforce subToken specific constraints (numeric or string) to update the current subToken bitmask
                        if ((currentSubTokenBitmask & intSubTokenBitmask) != 0) {
                            // integer subToken
                            if (currentSubTokenIntValue >= 0 && currentSubTokenIntValue < smallIntegerSubTokenUpperBound) {
                                // faster bitmask lookup for small integers
                                currentSubTokenBitmask = compiledSchema.smallIntegerSubTokenBitmasks[currentSubTokenIntValue];
                            } else {
                                currentSubTokenBitmask = findBitmaskForInteger(
                                    currentSubTokenIntValue,
                                    compiledSchema.integerSubTokenBitmaskArrayRanges,
                                    compiledSchema.integerSubTokenBitmasks
                                );
                            }
                        } else {
                            // general string subToken
                            ToIntFunction<SubstringView> subTokenBitmaskGenerator = null;
                            if (delimiterParsingInfo.bitmaskGeneratorPerPosition != null) {
                                subTokenBitmaskGenerator = delimiterParsingInfo.bitmaskGeneratorPerPosition[currentTokenSubTokenIndex];
                            }
                            if (subTokenBitmaskGenerator != null) {
                                substringView.set(currentSubTokenStartIndex, currentSubTokenEndIndex);
                                int substringBitmask = subTokenBitmaskGenerator.applyAsInt(substringView);
                                if (substringBitmask == 0) {
                                    // not a specific subToken, so we keep only the generic subToken types
                                    currentSubTokenBitmask &= genericSubTokenTypesBitmask;
                                } else {
                                    // the subToken is valid, so we set the bitmask to the evaluated value
                                    currentSubTokenBitmask &= substringBitmask;
                                    currentSubTokenIntValue = subTokenNumericValueRepresentationMap.applyAsInt(substringView);
                                }
                            } else {
                                // no bitmask generator for this subToken, meaning no known token expects this delimiter character
                                // at this position
                                currentSubTokenBitmask = 0;
                            }
                        }

                        // update the current token bitmask based on all "on" bits in the current sub-token bitmask
                        currentTokenBitmask &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(
                            currentSubTokenBitmask,
                            currentTokenSubTokenIndex
                        );
                    }

                    // buffer the current subToken info
                    if (currentSubTokenBitmask != 0) {
                        bufferedSubTokensIndex++;
                        bufferedSubTokenBitmasks[bufferedSubTokensIndex] = currentSubTokenBitmask;
                        bufferedSubTokenStartIndexes[bufferedSubTokensIndex] = currentSubTokenStartIndex;
                        bufferedSubTokenIntValues[bufferedSubTokensIndex] = currentSubTokenIntValue;
                        bufferedSubTokenLengths[bufferedSubTokensIndex] = currentSubTokenLength;
                        bufferedSubTokenSigns[bufferedSubTokensIndex] = currentSubTokenSignPrefix;
                        if (bufferedSubTokensIndex == maxSubTokensPerMultiToken - 1) {
                            // we are at the maximum number of subTokens for any known multi-token so we must flush the buffered info
                            flushBufferedInfo = true;
                        }
                    }

                    if (currentTokenStartIndex < 0) {
                        // ending the first sub-token of the current token
                        currentTokenStartIndex = currentSubTokenStartIndex;
                        currentTokenSubTokenStartIndex = bufferedSubTokensIndex;
                    }

                    boolean finalizeMultiToken = false;
                    if (charType == TOKEN_DELIMITER_CHAR_CODE || charType == LINE_END_CODE) {
                        int currentTokenLength = currentSubTokenEndIndex - currentTokenStartIndex;
                        if (currentTokenLength == 0) {
                            // empty tokens (consecutive white spaces) should have a zero bitmask, so we must change from the default
                            // non-zero
                            currentTokenBitmask = 0;
                        }

                        // eliminate token types with the wrong number of subTokens/tokens
                        currentTokenBitmask &= compiledSchema.subTokenCountToTokenBitmask[currentTokenSubTokenIndex];

                        if (currentTokenBitmask != 0) {
                            TokenType currentToken = tokenBitmaskRegistry.getHighestPriorityType(currentTokenBitmask);
                            bufferedTokensIndex++;
                            bufferedTokens[bufferedTokensIndex] = currentToken;
                            bufferedTokenStartIndexes[bufferedTokensIndex] = currentTokenStartIndex;
                            bufferedTokenLengths[bufferedTokensIndex] = currentTokenLength;
                            bufferedTokenSubTokenFirstIndexes[bufferedTokensIndex] = currentTokenSubTokenStartIndex;
                            bufferedTokenSubTokenLastIndexes[bufferedTokensIndex] = bufferedSubTokensIndex;

                            isBufferedTokenDecimalNumber[bufferedTokensIndex] = isPotentialDecimalNumber
                                && currentToken.encodingType() == EncodingType.DOUBLE
                                && currentTokenSubTokenIndex == 1
                                && (currentSubTokenBitmask & intSubTokenBitmask) != 0;

                            if (bufferedTokensIndex == 0) {
                                currentMultiTokenStartIndex = currentTokenStartIndex;
                            } else if (bufferedTokensIndex == maxTokensPerMultiToken - 1) {
                                // we reached the maximum number of tokens for any known multi-token, so we must flush the buffered info
                                flushBufferedInfo = true;
                            }

                            // update the current multi-token bitmask based on the current token
                            currentMultiTokenBitmask &= tokenBitmaskRegistry.getHigherLevelBitmaskByPosition(
                                currentTokenBitmask,
                                bufferedTokensIndex
                            );

                            if (currentMultiTokenBitmask != 0) {
                                // finalizing the current delimiter part, which would provide the indication whether it is
                                // time to finalize the current multi-token
                                int tmpDelimiterPartPosition = currentDelimiterPartPosition;
                                int tmpMultiTokenBitmask = currentMultiTokenBitmask;
                                if (currentSubTokenSuffixStartIndex >= 0) {
                                    for (int i = currentSubTokenSuffixStartIndex; i < indexWithinRawMessage; i++) {
                                        tmpDelimiterPartPosition++;
                                        CharSpecificParsingInfo suffixCharParsingInfo = charSpecificParsingInfos[rawMessage.charAt(i)];
                                        if (suffixCharParsingInfo != null) {
                                            int[] mtb2dpp = suffixCharParsingInfo.multiTokenBitmaskPerDelimiterPartPosition;
                                            if (mtb2dpp != null && tmpDelimiterPartPosition < mtb2dpp.length) {
                                                tmpMultiTokenBitmask &= mtb2dpp[tmpDelimiterPartPosition];
                                            } else {
                                                tmpMultiTokenBitmask = 0;
                                            }
                                        } else {
                                            tmpMultiTokenBitmask = 0;
                                        }
                                    }
                                }
                                tmpDelimiterPartPosition++;
                                if (delimiterParsingInfo.multiTokenBitmaskPerDelimiterPartPosition != null
                                    && tmpDelimiterPartPosition < delimiterParsingInfo.multiTokenBitmaskPerDelimiterPartPosition.length) {
                                    tmpMultiTokenBitmask &=
                                        delimiterParsingInfo.multiTokenBitmaskPerDelimiterPartPosition[tmpDelimiterPartPosition];
                                } else {
                                    tmpMultiTokenBitmask = 0;
                                }

                                if (tmpMultiTokenBitmask != 0) {
                                    // we are still within a valid multi-token parsing, so we can proceed to parse the next token
                                    currentMultiTokenBitmask = tmpMultiTokenBitmask;
                                    currentDelimiterPartPosition = tmpDelimiterPartPosition;

                                    if (charType == LINE_END_CODE) {
                                        // end of line reached - time to finalize the multi-token
                                        finalizeMultiToken = true;
                                    }
                                } else {
                                    if (bufferedTokensIndex > 0) {
                                        // having more that one token buffered and the switch from non-zero multi-token bitmask to zero
                                        // indicates that we should try to finalize the multi-token now
                                        finalizeMultiToken = true;
                                    }
                                    flushBufferedInfo = true;
                                }
                            } else {
                                flushBufferedInfo = true;
                            }
                        } else {
                            currentMultiTokenBitmask = 0;
                            flushBufferedInfo = true;
                        }
                        resetTokenState();
                    }

                    if (flushBufferedInfo || charType == LINE_END_CODE) {
                        int flushedTokens = 0;
                        int flushedSubTokens = 0;
                        if (finalizeMultiToken) {
                            // eliminate multi-token types with the wrong number of tokens or subTokens
                            currentMultiTokenBitmask &= compiledSchema.tokenCountToMultiTokenBitmask[bufferedTokensIndex];
                            currentMultiTokenBitmask &= compiledSchema.subTokenCountToMultiTokenBitmask[bufferedSubTokensIndex];

                            // eliminate multi-token types with the wrong delimiter parts total length
                            if (currentDelimiterPartPosition < delimiterPartsLengthToMultiTokenBitmask.length) {
                                currentMultiTokenBitmask &= delimiterPartsLengthToMultiTokenBitmask[currentDelimiterPartPosition];
                            } else {
                                currentMultiTokenBitmask = 0;
                            }

                            // todo - instead of all these thrown exceptions, we should log errors and continue parsing the best we can
                            if (currentMultiTokenBitmask != 0) {
                                MultiTokenType multiTokenType = multiTokenBitmaskRegistry.getUniqueType(currentMultiTokenBitmask);
                                if (multiTokenType != null) {
                                    Argument<?> argument = null;
                                    if (multiTokenType.getNumSubTokens() != bufferedSubTokensIndex + 1) {
                                        throw new IllegalStateException(
                                            String.format(
                                                Locale.ROOT,
                                                "Multi-token type %s expects %d subTokens, but there are only %d buffered subTokens",
                                                multiTokenType.name(),
                                                multiTokenType.getNumSubTokens(),
                                                bufferedSubTokensIndex + 1
                                            )
                                        );
                                    }
                                    if (multiTokenType.encodingType() == EncodingType.TIMESTAMP) {
                                        int currentMultiTokenEndIndex = bufferedTokenStartIndexes[bufferedTokensIndex]
                                            + bufferedTokenLengths[bufferedTokensIndex];
                                        int multiTokenLength = currentMultiTokenEndIndex - currentMultiTokenStartIndex;
                                        long timestampMillis = multiTokenType.getTimestampFormat().toTimestamp(bufferedSubTokenIntValues);
                                        argument = new Timestamp(
                                            currentMultiTokenStartIndex,
                                            multiTokenLength,
                                            timestampMillis,
                                            multiTokenType.getTimestampFormat().getJavaTimeFormat()
                                        );
                                    } else {
                                        throw new ParseException("Unknown multi-token type: " + multiTokenType.name());
                                    }
                                    if (argument != null) {
                                        templateArguments.addLast(argument);
                                        // multi-token generation consumes all buffered tokens and sub-tokens
                                        flushedTokens = bufferedTokensIndex + 1;
                                        flushedSubTokens = bufferedSubTokensIndex + 1;
                                    }
                                } else {
                                    throw new ParseException("Ambiguous multi-token type, schema should be changed");
                                }
                            } else {
                                // todo - invalid multi-token, we need to recalculate the buffered tokens and check if any sequence
                                // of them can form a valid multi-token. We should still
                            }
                            resetMultiTokenState();
                        }

                        // write each buffered token as a pattern argument
                        for (int i = flushedTokens; i <= bufferedTokensIndex; i++) {
                            TokenType tokenType = bufferedTokens[i];
                            Argument<?> argument = switch (tokenType.encodingType) {
                                case TIMESTAMP -> {
                                    if (i > 0) {
                                        // if we are here, it means that a valid multi-token timestamp was not formed and timestamp tokens
                                        // cannot be non-first in a multi-token
                                        throw new ParseException(
                                            "Timestamp token cannot be the non-first in a multi-token, but found at position " + i
                                        );
                                    }
                                    long timestampMillis = tokenType.getTimestampFormat().toTimestamp(bufferedSubTokenIntValues);
                                    yield new Timestamp(
                                        bufferedTokenStartIndexes[i],
                                        bufferedTokenLengths[i],
                                        timestampMillis,
                                        tokenType.getTimestampFormat().getJavaTimeFormat()
                                    );
                                }
                                case INTEGER -> {
                                    int integerSubTokenIndex = bufferedTokenSubTokenFirstIndexes[i];
                                    yield new IntegerArgument(
                                        bufferedTokenStartIndexes[i],
                                        bufferedTokenLengths[i],
                                        bufferedSubTokenIntValues[integerSubTokenIndex],
                                        bufferedSubTokenSigns[integerSubTokenIndex]
                                    );
                                }
                                case DOUBLE -> {
                                    if (isBufferedTokenDecimalNumber[i]) {
                                        // an optimization for simple decimal numbers - if we are here, it means that
                                        // this token contains a single decimal point and two integer sub-tokens
                                        int firstSubTokenIndex = bufferedTokenSubTokenFirstIndexes[i];
                                        int fractionalSubTokenLength = bufferedSubTokenLengths[firstSubTokenIndex + 1];
                                        int fractionalSubTokenIntValue = bufferedSubTokenIntValues[firstSubTokenIndex + 1];
                                        if (bufferedSubTokenSigns[firstSubTokenIndex] == Sign.MINUS) {
                                            fractionalSubTokenIntValue = -fractionalSubTokenIntValue;
                                        }
                                        double doubleValue = bufferedSubTokenIntValues[firstSubTokenIndex] + fractionalSubTokenIntValue
                                            / Math.pow(10, fractionalSubTokenLength);
                                        yield new DoubleArgument(bufferedTokenStartIndexes[i], bufferedTokenLengths[i], doubleValue);
                                    } else {
                                        // parse as a general double argument
                                        yield new DoubleArgument(rawMessage, bufferedTokenStartIndexes[i], bufferedTokenLengths[i]);
                                    }
                                }
                                case HEX -> new HexadecimalArgument(rawMessage, bufferedTokenStartIndexes[i], bufferedTokenLengths[i]);
                                case IPV4 -> new IPv4Argument(
                                    bufferedTokenStartIndexes[i],
                                    bufferedTokenLengths[i],
                                    bufferedSubTokenIntValues,
                                    bufferedTokenSubTokenFirstIndexes[i]
                                );
                                case UUID -> new UUIDArgument(rawMessage, bufferedTokenStartIndexes[i], bufferedTokenLengths[i]);
                                // todo - add support for local time arguments, relying on java.time.LocalTime
                                default -> null;
                            };
                            if (argument != null) {
                                templateArguments.addLast(argument);
                                flushedTokens++;
                                flushedSubTokens = bufferedTokenSubTokenLastIndexes[i] + 1;
                            }
                        }

                        // for each buffered subToken: if the subToken bitmask is !=0, add placeholder to the pattern and add the
                        // subToken as an argument,
                        // else write the subToken as is to the pattern, then add the corresponding delimiter character to the pattern
                        for (int i = flushedSubTokens; i <= bufferedSubTokensIndex; i++) {
                            Argument<?> argument = null;
                            // this is not about specific subToken types, here we are only dealing with generic subToken types, so we
                            // must switch off all specific subToken types, otherwise they have precedence
                            int subTokenBitmask = bufferedSubTokenBitmasks[i] & genericSubTokenTypesBitmask;
                            if (subTokenBitmask != 0) {
                                SubTokenType subTokenType = subTokenBitmaskRegistry.getHighestPriorityType(subTokenBitmask);
                                argument = switch (subTokenType.encodingType) {
                                    case INTEGER -> new IntegerArgument(
                                        bufferedSubTokenStartIndexes[i],
                                        bufferedSubTokenLengths[i],
                                        bufferedSubTokenIntValues[i],
                                        bufferedSubTokenSigns[i]
                                    );
                                    case DOUBLE -> new DoubleArgument(
                                        rawMessage,
                                        bufferedSubTokenStartIndexes[i],
                                        bufferedSubTokenLengths[i]
                                    );
                                    case HEX -> new HexadecimalArgument(
                                        rawMessage,
                                        bufferedSubTokenStartIndexes[i],
                                        bufferedSubTokenLengths[i]
                                    );
                                    default -> null;
                                };
                            } else if (isCurSubTokenContainsDigits) {
                                argument = new KeywordArgument(rawMessage, bufferedSubTokenStartIndexes[i], bufferedSubTokenLengths[i]);
                            }

                            if (argument != null) {
                                templateArguments.addLast(argument);
                            }
                        }
                        resetTokenState();
                        resetBuffers();
                    }
                    resetSubTokenState();
                    break;
                case TOKEN_BOUNDARY_CHAR_CODE:
                    if (currentSubTokenStartIndex == indexWithinRawMessage) {
                        // this is a sub-token prefix
                        currentSubTokenPrefixEndIndex = indexWithinRawMessage;
                        currentSubTokenStartIndex = -1;
                    } else if (currentSubTokenSuffixStartIndex < 0) {
                        // this is the first sub-token boundary suffix character
                        currentSubTokenSuffixStartIndex = indexWithinRawMessage;
                    }
                    break;
                default:
            }
        }
        return templateArguments;
    }

    /**
     * Finds the bitmask for the current integer value in the compiled schema through binary search.
     * See {@link CompiledSchema#integerSubTokenBitmaskArrayRanges} for details.
     * This method is separated so that it would be easily testable. It is very likely to be inlined by the JIT compiler, but consider
     * adding it directly to the parsing loop.
     * This algorithm assumest that the last entry in the integerSubTokenBitmaskArrayRanges is always {@link Integer#MAX_VALUE}.
     * @param value the integer value to find the bitmask for
     * @return the bitmask for the given integer value.
     */
    static int findBitmaskForInteger(final int value, final int[] integerSubTokenBitmaskArrayRanges, final int[] integerSubTokenBitmasks) {
        int low = 0;
        int high = integerSubTokenBitmaskArrayRanges.length - 1;
        while (low < high) {
            int mid = (low + high) / 2;
            if (integerSubTokenBitmaskArrayRanges[mid] < value) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return integerSubTokenBitmasks[low];
    }
}
