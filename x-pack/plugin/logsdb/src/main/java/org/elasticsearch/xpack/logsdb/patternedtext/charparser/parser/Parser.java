/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.CompiledSchema;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.HexadecimalArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.IPv4Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.IntegerArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.KeywordArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.PatternedMessage;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.Timestamp;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.UUIDArgument;

import java.util.ArrayList;
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
public final class Parser {

    private static final char ARGUMENT_PLACEHOLDER_PREFIX = '%';

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

    // todo: using char array is ~20X faster than using StringBuilder, good if we restrict ourselves to ASCII characters only for now
    // even if we want to support non-ASCII characters, writing to a char array and then appending the entire array to a StringBuilder
    // is still ~7X faster than using StringBuilder directly, so we can default to using char array and then convert it to StringBuilder
    // only if we encounter a non-ASCII character. For this, we can keep both a final char[] and a StringBuilder, and convert to
    // using the StringBuilder only when we encounter a non-ASCII character or we reach the maximum length of the char array and
    // recall the index of the last character written to the char array.
    //
    // todo: another option is to not create a template during parsing, only keep references to start and end indexes of the static parts
    // and the arguments, and then create the template only at the end of parsing. This also provides a way to maintain a state
    // that remember the length of the template as well as whether a non-ASCII character was encountered, so that only when
    // required we decide whether to use a char array or a StringBuilder.
    //
    // ongoing updated parsing results
    private final StringBuilder patternedMessage = new StringBuilder();
    private final List<Argument<?>> arguments = new ArrayList<>();
    private Timestamp timestamp = null;

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
    private final char[] bufferedSubTokenDelimiters;

    // current multi-token state
    int currentMultiTokenBitmask;
    int currentTokenIndex;
    final TokenType[] bufferedTokens;
    final int[] bufferedTokenBitmasks;
    private final char[] bufferedTokenDelimiters;
    private final int[] currentMultiTokenSubTokenValues;
    private int currentMultiTokenSubTokenIndex;

    public Parser(CompiledSchema compiledSchema) {
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
        bufferedSubTokenDelimiters = new char[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenBitmasks = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenIntValues = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenStartIndexes = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedSubTokenLengths = new int[compiledSchema.maxSubTokensPerToken + 1];
        bufferedTokens = new TokenType[compiledSchema.maxTokensPerMultiToken + 1];
        bufferedTokenBitmasks = new int[compiledSchema.maxTokensPerMultiToken + 1];
        bufferedTokenDelimiters = new char[compiledSchema.maxTokensPerMultiToken + 1];
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
        currentTokenIndex = -1;
        currentMultiTokenBitmask = allMultiTokenBitmask;
        currentMultiTokenSubTokenIndex = 0;
        // no need to reset arrays, we access them based on the currentTokenIndex anyway
    }

    private void reset() {
        patternedMessage.setLength(0);
        arguments.clear();
        timestamp = null;
        resetSubTokenState();
        resetTokenState();
        resetMultiTokenState();
    }

    /**
     * todo: explain the algorithm in high level
     * @param rawMessage the raw message to parse
     * @return a {@link PatternedMessage} object containing the parsed message and its arguments
     */
    public PatternedMessage parse(String rawMessage) {
        if (rawMessage == null || rawMessage.isEmpty()) {
            return new PatternedMessage("", null, new Argument<?>[0]);
        }
        reset();
        SubstringView substringView = new SubstringView(rawMessage);
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
                    //
                    // the flow is more or less as follows:
                    // 0. set a flag: flushBufferedInfo = false;
                    // todo - fix description, it is out of date
                    // 1. finalize the current subToken:
                    // a. if we reached the maximum number of subTokens for any known token - this token is invalid, set the current
                    // token bitmask to 0
                    // b. update current token bitmask with current subToken bitmask
                    // c. if the current token bitmask is 0 - set flushBufferedInfo = true and jump to buffers flush (invalid token
                    // cannot be part of a valid multi-token)
                    // d. else
                    // i. update the current token bitmask based on the current subToken type (integer or string) and position
                    // within the token
                    // ii. add the current subToken to the current token buffers
                    // iii. reset the current subToken state, as it is buffered now
                    // iv. update the current token bitmask based on the current subToken bitmask
                    // 2. if this is a token delimiter, finalize the current token:
                    // a. update the current token bitmask to include only the valid tokens for the current number of subTokens
                    // b. if the current token bitmask is 0 - set flushBufferedInfo = true and current multi-token bitmask = 0
                    // and jump to buffers flush
                    // c. else:
                    // i. try to create an argument from the current token and remember it as currentTokenArgument
                    // ii. if we reached the maximum number of tokens for any known multi-token, or the created argument is null, set
                    // the current multi token bitmask=0
                    // iii. else: update the current multi-token bitmask based on the multi-token bitmask that corresponds to the
                    // current token
                    // iii. if current multi-token bitmask ==0, set flushBufferedInfo=true and jump to buffers flush
                    // iv. else: add the current token to the multi-token buffer and set currentTokenArgument to null
                    // 3. if flushBufferedInfo == true, write all buffered info in the following order:
                    // a. if there are buffered tokens
                    // i. if the buffered tokens represent a valid multi-token (try creating one), write them as such
                    // ii. else, write each buffered token as a pattern argument
                    // iii. reset the multi-token buffers
                    // b. if currentTokenArgument != null, write it as a pattern argument and set it to null
                    // c. if there are buffered subTokens:
                    // i. write them either as is or as pattern arguments
                    // ii. reset the token buffers
                    // d. if the current subToken index is not 0, buffer it and reset the subToken
                    currentSubTokenIndex++;
                    bufferedSubTokenDelimiters[currentSubTokenIndex] = currentChar;
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
                            bufferedTokenDelimiters[currentTokenIndex] = currentChar;
                            bufferedTokenBitmasks[currentTokenIndex] = currentTokenBitmask;

                            if (currentTokenIndex == compiledSchema.maxTokensPerMultiToken) {
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
                                    createAndStoreTimestamp(multiTokenType);
                                } else {
                                    throw new IllegalStateException("Unknown multi-token type: " + multiTokenType.name());
                                }
                                // now fixing the buffers so that the last token becomes the only buffered token
                                bufferedTokens[0] = bufferedTokens[currentTokenIndex];
                                bufferedTokenBitmasks[0] = bufferedTokenBitmasks[currentTokenIndex];
                                bufferedTokenDelimiters[0] = bufferedTokenDelimiters[currentTokenIndex];
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
                                    createAndStoreTimestamp(tokenType);
                                } else {
                                    Argument<?> argument = switch (tokenType.encodingType) {
                                        // integer and hexadecimal arguments always contain a single subToken
                                        case INTEGER -> new IntegerArgument(bufferedSubTokenIntValues[0]);
                                        case HEX -> new HexadecimalArgument(
                                            rawMessage,
                                            bufferedSubTokenStartIndexes[0],
                                            bufferedSubTokenLengths[0]
                                        );
                                        case IPV4 -> new IPv4Argument(bufferedSubTokenIntValues);
                                        case UUID -> new UUIDArgument(rawMessage, currentTokenStartIndex, indexWithinRawMessage);
                                        default -> null;
                                    };
                                    if (argument != null) {
                                        arguments.add(argument);
                                        patternedMessage.append(ARGUMENT_PLACEHOLDER_PREFIX).append(argument.type().getSymbol());
                                    } else {
                                        // todo
                                    }
                                }
                                if (charType != LINE_END_CODE) {
                                    patternedMessage.append(bufferedTokenDelimiters[i]);
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
                                        case INTEGER -> new IntegerArgument(bufferedSubTokenIntValues[i]);
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
                                    arguments.add(argument);
                                    patternedMessage.append(ARGUMENT_PLACEHOLDER_PREFIX).append(argument.type().getSymbol());
                                } else {
                                    patternedMessage.append(rawMessage, bufferedSubTokenStartIndexes[i], indexWithinRawMessage);
                                }
                                if (charType != LINE_END_CODE) {
                                    patternedMessage.append(bufferedSubTokenDelimiters[i]);
                                }
                            }
                            resetTokenState();
                        }
                    }
                    resetSubTokenState();
                    break;
                case TRIMMED_CHAR_CODE:
                    // todo - remember the location and decide what to do - if we encounter it during a valid argument, then it needs to
                    // be buffered and eventually needs to be part of the argument (if it has a template) and not in the message template.
                    // However, if it is not encountered within a valid argument - it needs to be written to the template in its current
                    // location.
                    // In addition, we should probably trim these characters only at the beginning and end of tokens and sub-tokens.
                    patternedMessage.append(currentChar);
                    break;
                default:
            }
        }
        return new PatternedMessage(patternedMessage.toString(), timestamp, arguments.toArray(new Argument<?>[0]));
    }

    private void createAndStoreTimestamp(ParsingType parsingType) {
        TimestampFormat timestampFormat = parsingType.getTimestampFormat();
        long timestampMillis = timestampFormat.toTimestamp(currentMultiTokenSubTokenValues);
        Timestamp tmp = new Timestamp(timestampMillis, parsingType.getTimestampFormat().getJavaTimeFormat());
        if (timestamp == null) {
            timestamp = tmp;
        } else {
            arguments.addLast(tmp);
        }
        patternedMessage.append(ARGUMENT_PLACEHOLDER_PREFIX).append(EncodingType.TIMESTAMP.getSymbol());
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
