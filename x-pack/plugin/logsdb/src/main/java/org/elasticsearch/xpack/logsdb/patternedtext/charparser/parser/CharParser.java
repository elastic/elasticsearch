/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.HexadecimalArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IPv4Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IntegerArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.KeywordArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParseException;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Parser;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Timestamp;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.UUIDArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.CompiledSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.DIGIT_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.LINE_END_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TRIMMED_CHAR_CODE;

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

    private final SubTokenDelimiterCharParsingInfo[] subTokenDelimiterCharParsingInfos;

    // special bitmasks
    private final int intSubTokenBitmask;
    private final int allSubTokenBitmask;
    private final int allTokenBitmask;
    private final int allMultiTokenBitmask;

    // current subToken state
    private int currentSubTokenStartIndex;
    private int currentSubTokenLength;
    private int currentSubTokenBitmask;
    private int currentSubTokenIntValue;
    private boolean isCurSubTokenContainsDigits = false;

    // current token state
    private int currentTokenStartIndex;
    private int currentTokenBitmask;
    private int currentSubTokenIndex;
    private final int[] bufferedSubTokenBitmasks;
    private final int[] bufferedSubTokenIntValues;
    private final int[] bufferedSubTokenStartIndexes;
    private final int[] bufferedSubTokenLengths;

    // current multi-token state
    private int currentMultiTokenStartIndex;
    int currentMultiTokenBitmask;
    int currentTokenIndex;
    final TokenType[] bufferedTokens;
    final int[] bufferedTokenBitmasks;
    final int[] bufferedTokenStartIndexes;
    final int[] bufferedTokenLengths;
    private final int[] currentMultiTokenSubTokenValues;
    private int currentMultiTokenSubTokenIndex;

    public CharParser(CompiledSchema compiledSchema) {
        this.compiledSchema = compiledSchema;
        this.subTokenBitmaskRegistry = compiledSchema.subTokenBitmaskRegistry;
        this.tokenBitmaskRegistry = compiledSchema.tokenBitmaskRegistry;
        this.multiTokenBitmaskRegistry = compiledSchema.multiTokenBitmaskRegistry;
        this.charToSubTokenBitmask = compiledSchema.charToSubTokenBitmask;
        this.numUsedCharacters = this.charToSubTokenBitmask.length;
        this.charToCharType = compiledSchema.charToCharType;
        this.subTokenDelimiterCharParsingInfos = compiledSchema.subTokenDelimiterCharParsingInfos;
        this.intSubTokenBitmask = compiledSchema.intSubTokenBitmask;
        this.allSubTokenBitmask = subTokenBitmaskRegistry.getCombinedBitmask();
        this.allTokenBitmask = tokenBitmaskRegistry.getCombinedBitmask();
        this.allMultiTokenBitmask = multiTokenBitmaskRegistry.getCombinedBitmask();
        this.currentMultiTokenSubTokenValues = new int[compiledSchema.maxSubTokensPerMultiToken];
        bufferedSubTokenBitmasks = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenIntValues = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenStartIndexes = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenLengths = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedTokens = new TokenType[compiledSchema.maxTokensPerMultiToken + 1];
        bufferedTokenBitmasks = new int[compiledSchema.maxTokensPerMultiToken + 1];
        bufferedTokenStartIndexes = new int[compiledSchema.maxTokensPerMultiToken + 1];
        bufferedTokenLengths = new int[compiledSchema.maxTokensPerMultiToken + 1];
    }

    private void resetSubTokenState() {
        currentSubTokenStartIndex = -1;
        currentSubTokenLength = -1;
        currentSubTokenBitmask = allSubTokenBitmask;
        currentSubTokenIntValue = 0;
        isCurSubTokenContainsDigits = false;
    }

    private void resetTokenState() {
        currentTokenStartIndex = -1;
        currentTokenBitmask = allTokenBitmask;
        currentSubTokenIndex = -1;
        // no need to reset arrays, we access them based on the currentSubTokenIndex anyway
    }

    private void resetMultiTokenState() {
        currentMultiTokenStartIndex = -1;
        currentTokenIndex = -1;
        currentMultiTokenBitmask = allMultiTokenBitmask;
        currentMultiTokenSubTokenIndex = 0;
        // no need to reset arrays, we access them based on the currentTokenIndex anyway
    }

    private void reset() {
        resetSubTokenState();
        resetTokenState();
        resetMultiTokenState();
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
    public List<Argument<?>> parse(String rawMessage) throws ParseException {
        if (rawMessage == null || rawMessage.isEmpty()) {
            return Collections.emptyList();
        }
        reset();
        SubstringView substringView = new SubstringView(rawMessage);
        List<Argument<?>> finalArguments = new ArrayList<>();
        for (int indexWithinRawMessage = 0; indexWithinRawMessage <= rawMessage.length(); indexWithinRawMessage++) {
            byte charType;
            char currentChar;
            if (indexWithinRawMessage == rawMessage.length()) {
                currentChar = ' ';
                charType = LINE_END_CODE;
            } else {
                currentChar = rawMessage.charAt(indexWithinRawMessage);
                // todo: consider using 3 bits from the token bitmask for the character type, so that we can avoid the charToCharType lookup
                charType = charToCharType[currentChar];
            }

            if (currentSubTokenStartIndex < 0) {
                currentSubTokenStartIndex = indexWithinRawMessage;
            }
            if (currentTokenStartIndex < 0) {
                currentTokenStartIndex = indexWithinRawMessage;
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
                    // everything we need to do at the end of a subToken we also must do at the end of a token, so we share the logic
                    // in the next case
                case TOKEN_DELIMITER_CHAR_CODE:
                case LINE_END_CODE:
                    // whether we are processing a subToken delimiter or a token delimiter, once we encounter a state that invalidates
                    // the current parsed entity, we abort and write all buffered info (tokens and subTokens) to the pattern and/or
                    // as arguments in the right order: multi-token, token, subTokens.
                    // breaking from this case without writing the buffered info should happen only when we are still within a token
                    // parsing, or within a multi-token parsing.
                    currentSubTokenIndex++;
                    bufferedSubTokenStartIndexes[currentSubTokenIndex] = currentSubTokenStartIndex;
                    currentSubTokenLength = indexWithinRawMessage - currentSubTokenStartIndex;
                    bufferedSubTokenLengths[currentSubTokenIndex] = currentSubTokenLength;

                    boolean flushBufferedInfo = false;

                    if (currentSubTokenLength == 0) {
                        // empty tokens (for example, just "-") should have a zero bitmask, so we must change from the default non-zero
                        currentSubTokenBitmask = 0;
                    }

                    if (currentSubTokenIndex == compiledSchema.maxSubTokensPerToken) {
                        // we already passed the maximum number of subTokens for any known token
                        currentTokenBitmask = 0;
                    }

                    if (currentSubTokenBitmask == 0) {
                        // a token can only be composed of valid subTokens
                        currentTokenBitmask = 0;
                    }

                    if (currentTokenBitmask == 0) {
                        // no need to evaluate specific subToken types, the generic type would be enough
                        bufferedSubTokenBitmasks[currentSubTokenIndex] = currentSubTokenBitmask;
                        flushBufferedInfo = true;
                    } else {
                        SubTokenDelimiterCharParsingInfo subTokenDelimiterCharParsingInfo = subTokenDelimiterCharParsingInfos[currentChar];

                        // update the current token bitmask to include only valid tokens with the current delimiter character in the
                        // current subToken position
                        currentTokenBitmask &= subTokenDelimiterCharParsingInfo.tokenBitmaskPerSubTokenIndex[currentSubTokenIndex];

                        // here we enforce subToken specific constraints (numeric or string) to update the current subToken bitmask
                        if ((currentSubTokenBitmask & intSubTokenBitmask) != 0) {
                            if (currentSubTokenIntValue < compiledSchema.smallIntegerSubTokenBitmasks.length) {
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
                            SubTokenEvaluator<SubstringView> subTokenEvaluator =
                                subTokenDelimiterCharParsingInfo.subTokenEvaluatorPerSubTokenIndices[currentSubTokenIndex];
                            if (subTokenEvaluator != null) {
                                substringView.set(currentSubTokenStartIndex, indexWithinRawMessage);
                                int bitmaskNumericRepresentation = subTokenEvaluator.evaluate(substringView);
                                if (bitmaskNumericRepresentation < 0) {
                                    // the subToken is not valid, so we set the bitmask to 0
                                    currentSubTokenBitmask = 0;
                                } else {
                                    // the subToken is valid, so we set the bitmask to the evaluated value
                                    currentSubTokenBitmask &= subTokenEvaluator.bitmask;
                                    currentSubTokenIntValue = bitmaskNumericRepresentation;
                                }
                            } else {
                                // no bitmask generator for this subToken, meaning no known token expects this delimiter character
                                // at this position
                                currentSubTokenBitmask = 0;
                            }
                        }
                        bufferedSubTokenIntValues[currentSubTokenIndex] = currentSubTokenIntValue;
                        bufferedSubTokenBitmasks[currentSubTokenIndex] = currentSubTokenBitmask;

                        // update the current token bitmask based on all "on" bits in the current sub-token bitmask
                        currentTokenBitmask &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(
                            currentSubTokenBitmask,
                            currentSubTokenIndex
                        );
                    }

                    // handle token finalization
                    int formerMultiTokenBitmask = currentMultiTokenBitmask;
                    int formerMultiTokenEndIndex;
                    if (currentTokenIndex >= 0) {
                        formerMultiTokenEndIndex = bufferedTokenStartIndexes[currentTokenIndex] + bufferedTokenLengths[currentTokenIndex];
                    } else {
                        formerMultiTokenEndIndex = indexWithinRawMessage;
                    }

                    if (charType == TOKEN_DELIMITER_CHAR_CODE || charType == LINE_END_CODE) {
                        int currentTokenLength = indexWithinRawMessage - currentTokenStartIndex;
                        if (currentTokenLength == 0) {
                            // empty tokens (consecutive white spaces) should have a zero bitmask, so we must change from the default
                            // non-zero
                            currentTokenBitmask = 0;
                        }

                        // eliminate token/multi-token types with the wrong number of subTokens/tokens
                        currentTokenBitmask &= compiledSchema.subTokenCountToTokenBitmask[currentSubTokenIndex];

                        if (currentTokenBitmask != 0) {
                            TokenType currentToken = tokenBitmaskRegistry.getHighestPriorityType(currentTokenBitmask);
                            currentTokenIndex++;
                            bufferedTokens[currentTokenIndex] = currentToken;
                            bufferedTokenBitmasks[currentTokenIndex] = currentTokenBitmask;
                            bufferedTokenStartIndexes[currentTokenIndex] = currentTokenStartIndex;
                            bufferedTokenLengths[currentTokenIndex] = currentTokenLength;

                            if (currentTokenIndex == 0) {
                                currentMultiTokenStartIndex = currentTokenStartIndex;
                            } else if (currentTokenIndex == compiledSchema.maxTokensPerMultiToken) {
                                // we already passed the maximum number of tokens for any known multi-token
                                currentMultiTokenBitmask = 0;
                            } else {
                                // update the current multi-token bitmask based on the current token
                                currentMultiTokenBitmask &= tokenBitmaskRegistry.getHigherLevelBitmaskByPosition(
                                    currentTokenBitmask,
                                    currentTokenIndex
                                );
                            }

                            if (currentMultiTokenBitmask != 0) {
                                // copy the current subToken values to the multi-token subToken values
                                int numSubTokens = currentSubTokenIndex + 1;
                                if (currentSubTokenIndex >= 0) try {
                                    System.arraycopy(
                                        bufferedSubTokenIntValues,
                                        0,
                                        currentMultiTokenSubTokenValues,
                                        currentMultiTokenSubTokenIndex,
                                        numSubTokens
                                    );
                                } catch (IndexOutOfBoundsException e) {
                                    // invalid multi-token, we cannot fit all subTokens into the multi-token subToken values array
                                    currentMultiTokenBitmask = 0;
                                    flushBufferedInfo = true;
                                }
                                currentMultiTokenSubTokenIndex += numSubTokens;
                            } else {
                                flushBufferedInfo = true;
                            }

                            resetTokenState();
                        } else {
                            // todo - calculate floating point numbers if the current token looks like such. we can maintain a bitmask
                            // for floating point numbers that will be invalidated as soon as we encounter a character that
                            // is not defined in the "double" SubTokenBaseType, and then we can create a floating point argument
                            // from the buffered subTokens

                            currentMultiTokenBitmask = 0;
                            flushBufferedInfo = true;
                        }
                    }

                    if (flushBufferedInfo) {
                        // if we are here, we are not within a valid multi-token parsing (currentMultiTokenBitmask == 0) and not within a
                        // valid token parsing
                        if (currentMultiTokenBitmask == 0 && currentTokenIndex > 0) {
                            // there are at least two buffered tokens, the last of which is not related to the current multi-token, so
                            // we try to create a multi-token argument from the buffered tokens up to the current token index (exclusive).

                            // eliminate multi-token types with the wrong number of tokens or subTokens
                            formerMultiTokenBitmask &= compiledSchema.tokenCountToMultiTokenBitmask[currentTokenIndex];
                            if (currentMultiTokenSubTokenIndex >= 0) {
                                formerMultiTokenBitmask &= compiledSchema.subTokenCountToMultiTokenBitmask[currentMultiTokenSubTokenIndex
                                    - 1];
                            }

                            MultiTokenType multiTokenType = multiTokenBitmaskRegistry.getUniqueType(formerMultiTokenBitmask);
                            if (multiTokenType != null) {
                                if (multiTokenType.getNumSubTokens() != currentMultiTokenSubTokenIndex) {
                                    throw new IllegalStateException(
                                        String.format(
                                            Locale.ROOT,
                                            "Multi-token type %s expects %d subTokens, but there are only %d buffered subTokens",
                                            multiTokenType.name(),
                                            multiTokenType.getNumSubTokens(),
                                            currentMultiTokenSubTokenIndex
                                        )
                                    );
                                }
                                if (multiTokenType.encodingType() == EncodingType.TIMESTAMP) {
                                    int multiTokenLength = formerMultiTokenEndIndex - currentMultiTokenStartIndex;
                                    long timestampMillis = multiTokenType.getTimestampFormat().toTimestamp(currentMultiTokenSubTokenValues);
                                    finalArguments.addLast(
                                        new Timestamp(
                                            currentMultiTokenStartIndex,
                                            multiTokenLength,
                                            timestampMillis,
                                            multiTokenType.getTimestampFormat().getJavaTimeFormat()
                                        )
                                    );
                                } else {
                                    throw new ParseException("Unknown multi-token type: " + multiTokenType.name());
                                }

                                // now fixing the buffers so that the last token becomes the only buffered token
                                bufferedTokens[0] = bufferedTokens[currentTokenIndex];
                                bufferedTokenBitmasks[0] = bufferedTokenBitmasks[currentTokenIndex];
                                bufferedTokenStartIndexes[0] = bufferedTokenStartIndexes[currentTokenIndex];
                                bufferedTokenLengths[0] = bufferedTokenLengths[currentTokenIndex];
                                currentTokenIndex = 0;
                                currentMultiTokenSubTokenIndex = 0;
                            } else {
                                // todo - write buffered tokens and recalculate multi-token bitmask for the rest of the tokens
                            }
                        }

                        if (currentTokenIndex >= 0) {
                            // write each buffered token as a pattern argument
                            for (int i = 0; i <= currentTokenIndex; i++) {
                                TokenType tokenType = bufferedTokens[i];
                                if (tokenType.encodingType == EncodingType.TIMESTAMP) {
                                    long timestampMillis = tokenType.getTimestampFormat().toTimestamp(currentMultiTokenSubTokenValues);
                                    finalArguments.addLast(
                                        new Timestamp(
                                            bufferedTokenStartIndexes[i],
                                            bufferedTokenLengths[i],
                                            timestampMillis,
                                            tokenType.getTimestampFormat().getJavaTimeFormat()
                                        )
                                    );
                                } else {
                                    Argument<?> argument = switch (tokenType.encodingType) {
                                        // integer and hexadecimal arguments always contain a single subToken
                                        case INTEGER -> new IntegerArgument(
                                            bufferedTokenStartIndexes[i],
                                            bufferedTokenLengths[i],
                                            bufferedSubTokenIntValues[0]
                                        );
                                        case HEX -> new HexadecimalArgument(
                                            rawMessage,
                                            bufferedTokenStartIndexes[i],
                                            bufferedTokenLengths[i]
                                        );
                                        case IPV4 -> {
                                            if (currentTokenIndex == 0) {
                                                // IPv4 tokens can only be part of a single token, so we can safely create an IPv4 argument
                                                yield new IPv4Argument(
                                                    bufferedTokenStartIndexes[i],
                                                    bufferedTokenLengths[i],
                                                    bufferedSubTokenIntValues
                                                );
                                            } else {
                                                throw new ParseException(
                                                    "IPV4 token cannot be part of a multi-token, but found at position " + i
                                                );
                                            }
                                        }
                                        case UUID -> new UUIDArgument(rawMessage, bufferedTokenStartIndexes[i], bufferedTokenLengths[i]);
                                        default -> null;
                                    };
                                    if (argument != null) {
                                        finalArguments.addLast(argument);
                                    } else {
                                        // todo
                                    }
                                }
                            }
                            resetMultiTokenState();
                        }

                        // write the buffered subTokens
                        if (currentSubTokenIndex >= 0) {
                            // for each buffered subToken: if the subToken bitmask is !=0, add placeholder to the pattern and add the
                            // subToken as an argument,
                            // else write the subToken as is to the pattern, then add the corresponding delimiter character to the pattern
                            for (int i = 0; i <= currentSubTokenIndex; i++) {
                                Argument<?> argument = null;
                                // this is not about specific subToken types, here we are only dealing with generic subToken types, so we
                                // must switch off
                                // all specific subToken types, otherwise they have precedence
                                int subTokenBitmask = bufferedSubTokenBitmasks[i] & compiledSchema.genericSubTokenTypesBitmask;
                                if (subTokenBitmask != 0) {
                                    SubTokenType subTokenType = subTokenBitmaskRegistry.getHighestPriorityType(subTokenBitmask);
                                    argument = switch (subTokenType.encodingType) {
                                        case INTEGER -> new IntegerArgument(
                                            bufferedSubTokenStartIndexes[i],
                                            bufferedSubTokenLengths[i],
                                            bufferedSubTokenIntValues[i]
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
                                    finalArguments.addLast(argument);
                                } else {
                                    // A sub-token that is not an argument is a literal.
                                    // Its handling will be done in the final assembly phase.
                                }
                            }
                            resetTokenState();
                        }
                    }
                    resetSubTokenState();
                    break;
                case TRIMMED_CHAR_CODE:
                    // todo - check if this trimmed code in this position within a multi-token is valid
                    break;
                default:
            }
        }
        return finalArguments;
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
