/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.BitmaskRegistry;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.MultiTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenDelimiterCharParsingInfo;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenEvaluator;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringView;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TimestampFormat;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.MultiTokenFormat;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.Schema;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenBaseType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenFormat;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.IntConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.IntConstraints;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.ALPHABETIC_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.DIGIT_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.OTHER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TRIMMED_CHAR_CODE;

public class SchemaCompiler {

    public static final int ASCII_RANGE = 128;
    public static final int SMALL_INTEGERS_MAX_VALUE = 100;
    public static final String INTEGER_SUBTOKEN_NAME = "integer";
    public static final String HEX_SUBTOKEN_NAME = "hex";

    public static CompiledSchema compile(Schema schema) {

        byte[] charToCharType = new byte[ASCII_RANGE];
        for (int i = 0; i < ASCII_RANGE; i++) {
            charToCharType[i] = getCharCode((char) i, schema);
        }

        int[] charToSubTokenBitmask = new int[ASCII_RANGE];

        // for each delimiter char, we store the superset of token types that can be valid for this delimiter at each subToken index
        Map<Character, ArrayList<Integer>> delimiterCharToTokenBitmaskPerSubTokenIndex = new HashMap<>();
        // for each token format, the last subToken is not identified by a subToken delimiter, but rather by a token delimiter
        ArrayList<Integer> tokenBitmaskForLastSubToken = new ArrayList<>();
        // for each delimiter char, we store a list of sub token evaluators that can generate the token bitmask for this delimiter at
        // each subToken index within the token format.
        Map<Character, ArrayList<SubTokenEvaluator<SubstringView>>> delimiterCharToSubTokenEvaluatorPerSubTokenIndex = new HashMap<>();
        // for each token format, the last subToken is not identified by a subToken delimiter, but rather by a token delimiter
        ArrayList<SubTokenEvaluator<SubstringView>> subTokenEvaluatorForLastSubToken = new ArrayList<>();

        int allSubTokenBitmask = 0;
        int intSubTokenBitmask;
        int allIntegerSubTokenBitmask = 0;
        int genericSubTokenTypesBitmask = 0;

        Map<Integer, Integer> subTokenCountToTokenBitmaskMap = new HashMap<>();
        Map<Integer, Integer> tokenCountToMultiTokenBitmaskMap = new HashMap<>();
        Map<Integer, Integer> subTokenCountToMultiTokenBitmaskMap = new HashMap<>();

        int maxTokensPerMultiToken = 0;
        int maxSubTokensPerMultiToken = 0;
        Map<String, ArrayList<Integer>> tokenTypeToMultiTokenBitmaskByPosition = new HashMap<>();
        BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry = new BitmaskRegistry<>();
        for (org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.MultiTokenType multiTokenType : schema.getMultiTokenTypes()) {
            MultiTokenFormat format = multiTokenType.getFormat();
            List<org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType> tokens = format.getTokens();

            TimestampFormat timestampFormat = null;
            if (multiTokenType.encodingType() == EncodingType.TIMESTAMP) {
                timestampFormat = createTimestampFormat(format);
            }

            int subTokenCount = multiTokenType.getNumberOfSubTokens();
            int multiTokenBitmask = multiTokenBitmaskRegistry.register(
                new MultiTokenType(multiTokenType.name(), multiTokenType.encodingType(), subTokenCount, timestampFormat)
            );

            maxTokensPerMultiToken = Math.max(maxTokensPerMultiToken, tokens.size());
            maxSubTokensPerMultiToken = Math.max(maxSubTokensPerMultiToken, subTokenCount);

            int bitmaskForTokenCount = tokenCountToMultiTokenBitmaskMap.computeIfAbsent(tokens.size(), input -> 0);
            bitmaskForTokenCount |= multiTokenBitmask;
            tokenCountToMultiTokenBitmaskMap.put(tokens.size(), bitmaskForTokenCount);

            int bitmaskForSubTokenCount = subTokenCountToMultiTokenBitmaskMap.computeIfAbsent(subTokenCount, input -> 0);
            bitmaskForSubTokenCount |= multiTokenBitmask;
            subTokenCountToMultiTokenBitmaskMap.put(subTokenCount, bitmaskForSubTokenCount);

            for (int i = 0; i < tokens.size(); i++) {
                String tokenName = tokens.get(i).name();
                ArrayList<Integer> bitmaskList = tokenTypeToMultiTokenBitmaskByPosition.computeIfAbsent(
                    tokenName,
                    input -> new ArrayList<>()
                );
                fillListUpToIndex(bitmaskList, i, () -> 0);
                bitmaskList.set(i, bitmaskList.get(i) | multiTokenBitmask);
            }
        }
        multiTokenBitmaskRegistry.seal();

        int maxSubTokensPerToken = 0;
        Map<String, ArrayList<Integer>> subTokenTypeToTokenBitmaskByPosition = new HashMap<>();
        // a way to get the positions of subTokens and the corresponding delimiter characters within all token formats
        Map<
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType,
            Map<Character, Set<Integer>>> subTokenTypeToDelimiterCharToPositions = new HashMap<>();
        // a way to get the indices of the last subTokens within all token formats
        Map<org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType, Set<Integer>> subTokenTypeToLastSubTokenIndex =
            new HashMap<>();
        BitmaskRegistry<TokenType> tokenBitmaskRegistry = new BitmaskRegistry<>();

        // register generic token types first, as they would have the lowest priority
        // generic token types are not part of multi-token types, so they are registered with an empty multi-token bitmask
        tokenBitmaskRegistry.register(new TokenType(INTEGER_SUBTOKEN_NAME, EncodingType.INTEGER, 1, null, new int[maxTokensPerMultiToken]));
        tokenBitmaskRegistry.register(new TokenType("double", EncodingType.DOUBLE, 2, null, new int[maxTokensPerMultiToken]));
        tokenBitmaskRegistry.register(new TokenType(HEX_SUBTOKEN_NAME, EncodingType.HEX, 1, null, new int[maxTokensPerMultiToken]));

        for (org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType tokenType : schema.getTokenTypes()) {
            ArrayList<Integer> multiTokenBitmaskByPositionList = tokenTypeToMultiTokenBitmaskByPosition.get(tokenType.name());
            int[] multiTokenBitmaskByPosition;
            if (multiTokenBitmaskByPositionList == null) {
                multiTokenBitmaskByPosition = new int[maxTokensPerMultiToken];
            } else {
                fillListUpToIndex(multiTokenBitmaskByPositionList, maxTokensPerMultiToken - 1, () -> 0);
                multiTokenBitmaskByPosition = new int[multiTokenBitmaskByPositionList.size()];
                for (int i = 0; i < multiTokenBitmaskByPositionList.size(); i++) {
                    multiTokenBitmaskByPosition[i] = multiTokenBitmaskByPositionList.get(i);
                }
            }

            TimestampFormat timestampFormat = null;
            if (tokenType.encodingType() == EncodingType.TIMESTAMP) {
                timestampFormat = createTimestampFormat(tokenType);
            }

            int subTokenCount = tokenType.getNumberOfSubTokens();
            int tokenBitmask = tokenBitmaskRegistry.register(
                new TokenType(tokenType.name(), tokenType.encodingType(), subTokenCount, timestampFormat, multiTokenBitmaskByPosition)
            );

            int bitmaskForSubTokenCount = subTokenCountToTokenBitmaskMap.computeIfAbsent(subTokenCount, input -> 0);
            bitmaskForSubTokenCount |= tokenBitmask;
            subTokenCountToTokenBitmaskMap.put(subTokenCount, bitmaskForSubTokenCount);

            maxSubTokensPerToken = Math.max(maxSubTokensPerToken, subTokenCount);
            TokenFormat format = tokenType.format();
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType[] subTokenTypes = format.getSubTokenTypes();
            for (int i = 0; i < subTokenTypes.length; i++) {
                org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subTokenType = subTokenTypes[i];
                String subTokenName = subTokenType.name();
                ArrayList<Integer> bitmaskList = subTokenTypeToTokenBitmaskByPosition.computeIfAbsent(
                    subTokenName,
                    input -> new ArrayList<>()
                );
                fillListUpToIndex(bitmaskList, i, () -> 0);
                bitmaskList.set(i, bitmaskList.get(i) | tokenBitmask);

                char[] subTokenDelimiters = format.getSubTokenDelimiters();
                if (i < subTokenDelimiters.length) {
                    char subTokenDelimiter = subTokenDelimiters[i];
                    ArrayList<Integer> tokenBitmaskPerSubTokenIndex = delimiterCharToTokenBitmaskPerSubTokenIndex.computeIfAbsent(
                        subTokenDelimiter,
                        input -> new ArrayList<>()
                    );
                    fillListUpToIndex(tokenBitmaskPerSubTokenIndex, i, () -> 0);
                    tokenBitmaskPerSubTokenIndex.set(i, tokenBitmaskPerSubTokenIndex.get(i) | tokenBitmask);

                    Map<Character, Set<Integer>> delimiterCharToPositions = subTokenTypeToDelimiterCharToPositions.computeIfAbsent(
                        subTokenType,
                        input -> new HashMap<>()
                    );
                    Set<Integer> positions = delimiterCharToPositions.computeIfAbsent(subTokenDelimiter, input -> new HashSet<>());
                    positions.add(i);
                } else {
                    fillListUpToIndex(tokenBitmaskForLastSubToken, i, () -> 0);
                    tokenBitmaskForLastSubToken.set(i, tokenBitmaskForLastSubToken.get(i) | tokenBitmask);

                    Set<Integer> lastSubTokenIndices = subTokenTypeToLastSubTokenIndex.computeIfAbsent(
                        subTokenType,
                        input -> new HashSet<>()
                    );
                    lastSubTokenIndices.add(i);
                }
            }
        }
        tokenBitmaskRegistry.seal();

        BitmaskRegistry<SubTokenType> subTokenBitmaskRegistry = new BitmaskRegistry<>();

        // Register generic subToken types first, as they would have the lowest priority
        intSubTokenBitmask = subTokenBitmaskRegistry.register(
            new SubTokenType(INTEGER_SUBTOKEN_NAME, EncodingType.INTEGER, new int[maxSubTokensPerToken], TimestampComponentType.NA)
        );
        allIntegerSubTokenBitmask |= intSubTokenBitmask;
        genericSubTokenTypesBitmask |= intSubTokenBitmask;
        allSubTokenBitmask |= intSubTokenBitmask;
        ArrayList<SubTokenBaseType> subTokenBaseTypes = schema.getSubTokenBaseTypes();
        SubTokenBaseType integerSubTokenBaseType = subTokenBaseTypes.stream()
            .filter(baseType -> baseType.name().equals("unsigned_integer"))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Integer subToken base type not found in schema"));
        updateCharToSubTokenBitmasks(
            INTEGER_SUBTOKEN_NAME,
            charToSubTokenBitmask,
            integerSubTokenBaseType.allowedCharacters(),
            intSubTokenBitmask
        );

        int hexSubTokenBitmask = subTokenBitmaskRegistry.register(
            new SubTokenType(HEX_SUBTOKEN_NAME, EncodingType.HEX, new int[maxSubTokensPerToken], TimestampComponentType.NA)
        );
        genericSubTokenTypesBitmask |= hexSubTokenBitmask;
        allSubTokenBitmask |= hexSubTokenBitmask;
        SubTokenBaseType hexSubTokenBaseType = subTokenBaseTypes.stream()
            .filter(baseType -> baseType.name().equals("hexadecimal"))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Hex subToken base type not found in schema"));
        updateCharToSubTokenBitmasks(HEX_SUBTOKEN_NAME, charToSubTokenBitmask, hexSubTokenBaseType.allowedCharacters(), hexSubTokenBitmask);

        int[] smallIntegerSubTokenBitmasks = new int[SMALL_INTEGERS_MAX_VALUE + 1];
        for (int i = 0; i <= SMALL_INTEGERS_MAX_VALUE; i++) {
            smallIntegerSubTokenBitmasks[i] = intSubTokenBitmask;
        }
        ArrayList<IntRangeBitmask> intRangeBitmasks = new ArrayList<>();

        for (org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subTokenType : schema.getSubTokenTypes()) {
            ArrayList<Integer> tokenBitmaskByPositionList = subTokenTypeToTokenBitmaskByPosition.get(subTokenType.name());
            int[] tokenBitmaskByPosition;
            if (tokenBitmaskByPositionList == null) {
                tokenBitmaskByPosition = new int[maxSubTokensPerToken];
            } else {
                fillListUpToIndex(tokenBitmaskByPositionList, maxSubTokensPerToken - 1, () -> 0);
                tokenBitmaskByPosition = new int[tokenBitmaskByPositionList.size()];
                for (int i = 0; i < tokenBitmaskByPositionList.size(); i++) {
                    tokenBitmaskByPosition[i] = tokenBitmaskByPositionList.get(i);
                }
            }
            EncodingType encodingType = subTokenType.encodingType();
            int subTokenBitmask = subTokenBitmaskRegistry.register(
                new SubTokenType(subTokenType.name(), encodingType, tokenBitmaskByPosition, subTokenType.getTimestampComponentType())
            );
            if (encodingType == EncodingType.INTEGER) {
                allIntegerSubTokenBitmask |= subTokenBitmask;
            }
            updateCharToSubTokenBitmasks(subTokenType.name(), charToSubTokenBitmask, subTokenType.getValidCharacters(), subTokenBitmask);
            allSubTokenBitmask |= subTokenBitmask;

            IntConstraint intConstraint = subTokenType.getIntConstraint();
            if (intConstraint != null) {
                for (int i = 0; i < smallIntegerSubTokenBitmasks.length; i++) {
                    if (intConstraint.isApplicable(i)) {
                        smallIntegerSubTokenBitmasks[i] |= subTokenBitmask;
                    }
                }
                for (IntConstraints.Range range : intConstraint.trueRanges()) {
                    intRangeBitmasks.add(new IntRangeBitmask(range, subTokenBitmask));
                }
            }

            StringConstraint stringConstraint = subTokenType.getStringConstraint();
            if (stringConstraint != null) {
                SubTokenEvaluator<SubstringView> tokenSubTokenEvaluator = SubTokenEvaluatorFactory.from(subTokenBitmask, stringConstraint);
                Map<Character, Set<Integer>> delimiterCharToPositions = subTokenTypeToDelimiterCharToPositions.get(subTokenType);
                if (delimiterCharToPositions != null) {
                    for (Map.Entry<Character, Set<Integer>> entry : delimiterCharToPositions.entrySet()) {
                        char subTokenDelimiter = entry.getKey();
                        ArrayList<SubTokenEvaluator<SubstringView>> subTokenEvaluatorPerSubTokenIndex =
                            delimiterCharToSubTokenEvaluatorPerSubTokenIndex.computeIfAbsent(subTokenDelimiter, input -> new ArrayList<>());
                        Set<Integer> positions = entry.getValue();
                        positions.forEach(position -> {
                            fillListUpToIndex(subTokenEvaluatorPerSubTokenIndex, position, () -> null);
                            SubTokenEvaluator<SubstringView> existingEvaluator = subTokenEvaluatorPerSubTokenIndex.get(position);
                            subTokenEvaluatorPerSubTokenIndex.set(
                                position,
                                // todo - the usage of or operator is flawed here, as it uses a combined bitmask for the chained evaluators,
                                // while what we really need is a chain the returns the combined bitmask of only evaluators that produce
                                // a non-negative value
                                existingEvaluator == null ? tokenSubTokenEvaluator : existingEvaluator.or(tokenSubTokenEvaluator)
                            );
                        });
                    }
                }
                // for the last subToken, we use the token delimiter to identify it, so we add it to the subTokenEvaluatorForLastSubToken
                Set<Integer> lastPositionIndices = subTokenTypeToLastSubTokenIndex.get(subTokenType);
                if (lastPositionIndices != null) {
                    lastPositionIndices.forEach(position -> {
                        fillListUpToIndex(subTokenEvaluatorForLastSubToken, position, () -> null);
                        SubTokenEvaluator<SubstringView> existingEvaluator = subTokenEvaluatorForLastSubToken.get(position);
                        subTokenEvaluatorForLastSubToken.set(
                            position,
                            // todo - the usage of or operator is flawed here, as it uses a combined bitmask for the chained evaluators,
                            // while what we really need is a chain the returns the combined bitmask of only evaluators that produce
                            // a non-negative value
                            existingEvaluator == null ? tokenSubTokenEvaluator : existingEvaluator.or(tokenSubTokenEvaluator)
                        );
                    });
                }
            }
        }
        subTokenBitmaskRegistry.seal();

        SubTokenDelimiterCharParsingInfo[] subTokenDelimiterCharParsingInfos = new SubTokenDelimiterCharParsingInfo[ASCII_RANGE];

        for (char delimiter : schema.getSubTokenDelimiters()) {
            if (delimiter < ASCII_RANGE) {
                // subToken delimiter characters should be mapped to the inclusive subToken bitmask, as they are valid to all subToken types
                charToSubTokenBitmask[delimiter] = allSubTokenBitmask;
                ArrayList<Integer> tokenBitmaskPerSubTokenIndexArray = delimiterCharToTokenBitmaskPerSubTokenIndex.get(delimiter);
                int[] tokenBitmaskPerSubTokenIndex = new int[maxSubTokensPerToken];
                if (tokenBitmaskPerSubTokenIndexArray != null) {
                    fillListUpToIndex(tokenBitmaskPerSubTokenIndexArray, maxSubTokensPerToken - 1, () -> 0);
                    for (int i = 0; i < tokenBitmaskPerSubTokenIndexArray.size(); i++) {
                        tokenBitmaskPerSubTokenIndex[i] = tokenBitmaskPerSubTokenIndexArray.get(i);
                    }
                    tokenBitmaskPerSubTokenIndex = tokenBitmaskPerSubTokenIndexArray.stream().mapToInt(Integer::intValue).toArray();
                }
                ArrayList<SubTokenEvaluator<SubstringView>> tokenSubTokenEvaluatorPerSubTokenIndexArray =
                    delimiterCharToSubTokenEvaluatorPerSubTokenIndex.get(delimiter);
                @SuppressWarnings({ "unchecked", "rawtypes" })
                SubTokenEvaluator<SubstringView>[] tokenSubTokenEvaluatorPerSubTokenIndices = new SubTokenEvaluator[maxSubTokensPerToken];
                if (tokenSubTokenEvaluatorPerSubTokenIndexArray != null) {
                    fillListUpToIndex(tokenSubTokenEvaluatorPerSubTokenIndexArray, maxSubTokensPerToken - 1, () -> null);
                    for (int i = 0; i < tokenSubTokenEvaluatorPerSubTokenIndexArray.size(); i++) {
                        tokenSubTokenEvaluatorPerSubTokenIndices[i] = tokenSubTokenEvaluatorPerSubTokenIndexArray.get(i);
                    }
                }
                subTokenDelimiterCharParsingInfos[delimiter] = new SubTokenDelimiterCharParsingInfo(
                    delimiter,
                    tokenBitmaskPerSubTokenIndex,
                    tokenSubTokenEvaluatorPerSubTokenIndices
                );
            } else {
                throw new IllegalArgumentException(
                    "SubToken delimiter character '" + delimiter + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        int[] tokenBitmaskPerSubTokenIndex = tokenBitmaskForLastSubToken.stream().mapToInt(Integer::intValue).toArray();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        SubTokenEvaluator<SubstringView>[] subTokenEvaluatorPerSubTokenIndex = subTokenEvaluatorForLastSubToken.toArray(
            new SubTokenEvaluator[0]
        );
        for (char delimiter : schema.getTokenDelimiters()) {
            if (delimiter < ASCII_RANGE) {
                // token delimiter characters are also subToken delimiters (that delimit between the last subToken to the next token)
                // and therefore should be mapped to the inclusive subToken bitmask, as they are valid to all subToken types
                charToSubTokenBitmask[delimiter] = allSubTokenBitmask;
                SubTokenDelimiterCharParsingInfo tokenDelimiterCharParsingInfo = new SubTokenDelimiterCharParsingInfo(
                    delimiter,
                    tokenBitmaskPerSubTokenIndex,
                    subTokenEvaluatorPerSubTokenIndex
                );
                subTokenDelimiterCharParsingInfos[delimiter] = tokenDelimiterCharParsingInfo;
            } else {
                throw new IllegalArgumentException(
                    "Token delimiter character '" + delimiter + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        for (char trimmedChar : schema.getTrimmedCharacters()) {
            if (trimmedChar < ASCII_RANGE) {
                // trimmed characters should not invalidate any sub-token, so we use the inclusive sub-token bitmask
                charToSubTokenBitmask[trimmedChar] = allSubTokenBitmask;
            } else {
                throw new IllegalArgumentException(
                    "Trimmed character '" + trimmedChar + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        int[] subTokenCountToTokenBitmask = new int[maxSubTokensPerToken];
        for (Map.Entry<Integer, Integer> entry : subTokenCountToTokenBitmaskMap.entrySet()) {
            int subTokenCount = entry.getKey();
            int bitmask = entry.getValue();
            if (subTokenCount <= subTokenCountToTokenBitmask.length) {
                subTokenCountToTokenBitmask[subTokenCount - 1] = bitmask;
            } else {
                throw new IllegalStateException(
                    "Sub-token count "
                        + subTokenCount
                        + " exceeds the size of the subTokenCountToTokenBitmask array ("
                        + subTokenCountToTokenBitmask.length
                        + "). This may lead to unexpected behavior."
                );
            }
        }

        int[] tokenCountToMultiTokenBitmask = new int[maxTokensPerMultiToken];
        for (Map.Entry<Integer, Integer> entry : tokenCountToMultiTokenBitmaskMap.entrySet()) {
            int tokenCount = entry.getKey();
            int bitmask = entry.getValue();
            if (tokenCount <= tokenCountToMultiTokenBitmask.length) {
                tokenCountToMultiTokenBitmask[tokenCount - 1] = bitmask;
            } else {
                throw new IllegalStateException(
                    "Token count "
                        + tokenCount
                        + " exceeds the size of the tokenCountToMultiTokenBitmaskArray ("
                        + tokenCountToMultiTokenBitmask.length
                        + "). This may lead to unexpected behavior."
                );
            }
        }

        int[] subTokenCountToMultiTokenBitmask = new int[maxSubTokensPerMultiToken];
        for (Map.Entry<Integer, Integer> entry : subTokenCountToMultiTokenBitmaskMap.entrySet()) {
            int subTokenCount = entry.getKey();
            int bitmask = entry.getValue();
            if (subTokenCount <= subTokenCountToMultiTokenBitmask.length) {
                subTokenCountToMultiTokenBitmask[subTokenCount - 1] = bitmask;
            } else {
                throw new IllegalStateException(
                    "Sub-token count "
                        + subTokenCount
                        + " exceeds the size of the subTokenCountToMultiTokenBitmaskArray ("
                        + subTokenCountToMultiTokenBitmask.length
                        + "). This may lead to unexpected behavior."
                );
            }
        }

        intRangeBitmasks = mergeIntRangeBitmasks(intRangeBitmasks);
        int[] integerSubTokenBitmaskArrayRanges = new int[intRangeBitmasks.size()];
        int[] integerSubTokenBitmasks = new int[intRangeBitmasks.size()];
        for (int i = 0; i < intRangeBitmasks.size(); i++) {
            IntRangeBitmask intRangeBitmask = intRangeBitmasks.get(i);
            integerSubTokenBitmaskArrayRanges[i] = intRangeBitmask.range().upperBound();
            // ensure that the generic integer subToken bit is always set
            integerSubTokenBitmasks[i] = intRangeBitmask.bitmask() | intSubTokenBitmask;
        }

        return new CompiledSchema(
            charToSubTokenBitmask,
            charToCharType,
            subTokenDelimiterCharParsingInfos,
            maxSubTokensPerToken,
            maxTokensPerMultiToken,
            maxSubTokensPerMultiToken,
            intSubTokenBitmask,
            allIntegerSubTokenBitmask,
            genericSubTokenTypesBitmask,
            integerSubTokenBitmasks,
            integerSubTokenBitmaskArrayRanges,
            smallIntegerSubTokenBitmasks,
            subTokenCountToTokenBitmask,
            tokenCountToMultiTokenBitmask,
            subTokenCountToMultiTokenBitmask,
            subTokenBitmaskRegistry,
            tokenBitmaskRegistry,
            multiTokenBitmaskRegistry
        );
    }

    public static byte getCharCode(char c, Schema schema) {
        if (Character.isDigit(c)) {
            return DIGIT_CHAR_CODE;
        } else if (Character.isAlphabetic(c)) {
            return ALPHABETIC_CHAR_CODE;
        }

        for (char delim : schema.getSubTokenDelimiters()) {
            if (c == delim) {
                return SUBTOKEN_DELIMITER_CHAR_CODE;
            }
        }

        for (char delim : schema.getTokenDelimiters()) {
            if (c == delim) {
                return TOKEN_DELIMITER_CHAR_CODE;
            }
        }

        for (char trimmed : schema.getTrimmedCharacters()) {
            if (c == trimmed) {
                return TRIMMED_CHAR_CODE;
            }
        }

        return OTHER_CHAR_CODE;
    }

    private static void updateCharToSubTokenBitmasks(
        String subTokenTypeName,
        int[] charToSubTokenBitmask,
        char[] validCharacters,
        int subTokenBitmask
    ) {
        for (char c : validCharacters) {
            if (c < ASCII_RANGE) {
                charToSubTokenBitmask[c] |= subTokenBitmask;
            } else {
                throw new IllegalArgumentException(
                    "Character '"
                        + c
                        + "' in subToken type '"
                        + subTokenTypeName
                        + "' is outside the ASCII range and will not be processed."
                );
            }
        }
    }

    private static <T> void fillListUpToIndex(ArrayList<T> list, int index, Supplier<T> supplier) {
        while (list.size() <= index) {
            list.addLast(supplier.get());
        }
    }

    // ===================================================== int ranges =============================================================

    /**
     * Merges overlapping ranges in a list of {@link IntRangeBitmask} instances by combining their bitmasks using a bitwise OR operation.
     * The resulting list will contain ordered non-overlapping ranges of integers, where each range is associated with a single bitmask.
     * @param intRangeBitmasks the list of IntRangeBitmask to reduce
     * @return an ordered list of non-overlapping {@link IntRangeBitmask} instances representing the merged ranges
     */
    static ArrayList<IntRangeBitmask> mergeIntRangeBitmasks(ArrayList<IntRangeBitmask> intRangeBitmasks) {
        ArrayList<RangeBoundary> boundaries = new ArrayList<>();
        for (IntRangeBitmask intRangeBitmask : intRangeBitmasks) {
            boundaries.add(new RangeBoundary(intRangeBitmask.range().lowerBound(), true, intRangeBitmask.bitmask()));
            boundaries.add(new RangeBoundary(intRangeBitmask.range().upperBound(), false, intRangeBitmask.bitmask()));
        }
        // sort boundaries by boundary value, lower bounds first
        // noinspection Java8ListSort - cannot use List.sort here due to poor checkstyle algorithm
        Collections.sort(
            boundaries,
            Comparator.comparingInt(RangeBoundary::boundary).thenComparing(RangeBoundary::isLowerBound, Comparator.reverseOrder())
        );

        ArrayList<IntRangeBitmask> reducedRanges = new ArrayList<>();

        int accumulatedBitmask = 0;
        Integer activeRangeStart = Integer.MIN_VALUE;
        Integer activeRangeEnd = null;
        Integer activeRangeBitmask = null;

        for (RangeBoundary rangeBoundary : boundaries) {
            int boundary = rangeBoundary.boundary();

            // if there is already a finished current range, add it to the result
            if (activeRangeEnd != null && boundary > activeRangeEnd) {
                reducedRanges.addLast(IntRangeBitmask.of(activeRangeStart, activeRangeEnd, activeRangeBitmask));
                // reset the current range
                activeRangeStart = activeRangeEnd + 1;
                activeRangeEnd = null;
                activeRangeBitmask = null;
            }

            if (rangeBoundary.isLowerBound()) {
                // enter a range
                if (boundary > activeRangeStart) {
                    // if the boundary is greater than the current range start, we should close this range and add it to the result
                    reducedRanges.addLast(IntRangeBitmask.of(activeRangeStart, boundary - 1, accumulatedBitmask));
                    // start a new range from the current boundary
                    activeRangeStart = boundary;
                }
                // accumulate the bitmask for the current range
                accumulatedBitmask |= rangeBoundary.bitmask();
            } else {
                // exit a range
                activeRangeEnd = boundary;
                if (activeRangeBitmask == null) {
                    activeRangeBitmask = accumulatedBitmask;
                }
                // remove the range's bitmask from the accumulated bitmask
                accumulatedBitmask &= ~rangeBoundary.bitmask();
            }
        }
        if (activeRangeEnd != null) {
            reducedRanges.addLast(IntRangeBitmask.of(activeRangeStart, activeRangeEnd, activeRangeBitmask));
            if (activeRangeEnd < Integer.MAX_VALUE) {
                activeRangeStart = activeRangeEnd + 1;
            } else {
                activeRangeStart = null;
            }
        }
        if (activeRangeStart != null) {
            reducedRanges.addLast(IntRangeBitmask.of(activeRangeStart, Integer.MAX_VALUE, accumulatedBitmask));
        }
        return reducedRanges;
    }

    record IntRangeBitmask(IntConstraints.Range range, int bitmask) {
        static IntRangeBitmask of(int lowerBound, int upperBound, int bitmask) {
            return new IntRangeBitmask(new IntConstraints.Range(lowerBound, upperBound), bitmask);
        }

        @Override
        public String toString() {
            return "IntRangeBitmask{" + "range=" + range + ", bitmask=" + Integer.toBinaryString(bitmask) + '}';
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other instanceof IntRangeBitmask) {
                @SuppressWarnings("PatternVariableCanBeUsed") // cannot use pattern variable due to poor checkstyle algorithm
                IntRangeBitmask otherRange = (IntRangeBitmask) other;
                return range.equals(otherRange.range()) && bitmask == otherRange.bitmask();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return range.lowerBound() + range.upperBound() + bitmask;
        }
    }

    private record RangeBoundary(int boundary, boolean isLowerBound, int bitmask) {}

    // =================================================== Timestamp formatting ========================================================

    /**
     * Creates a {@link TimestampFormat} from a {@link MultiTokenFormat} object that represents timestamp components.
     * This method processes both token parts and literal string parts to construct the final Java time format.
     * @param format The MultiTokenFormat object representing the timestamp format.
     * @return A TimestampFormat object containing the format string and an array indicating the order of timestamp components.
     */
    static TimestampFormat createTimestampFormat(MultiTokenFormat format) {
        StringBuilder javaTimeFormat = new StringBuilder();
        int[] timestampComponentsOrder = new int[TimestampComponentType.values().length];
        Arrays.fill(timestampComponentsOrder, -1);
        int nextComponentIndex = 0;
        for (Object part : format.getFormatParts()) {
            if (part instanceof String) {
                for (char c : ((String) part).toCharArray()) {
                    appendDelimiter(javaTimeFormat, c);
                }
            } else if (part instanceof org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType token) {
                StringBuilder tokenJavaTimeFormat = new StringBuilder();
                nextComponentIndex += appendTimestampComponents(token, tokenJavaTimeFormat, timestampComponentsOrder, nextComponentIndex);
                javaTimeFormat.append(tokenJavaTimeFormat);
            }
        }
        return new TimestampFormat(javaTimeFormat.toString(), timestampComponentsOrder);
    }

    /**
     * Creates a {@link TimestampFormat} from a single {@link org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType}
     * object that represents a timestamp format.
     * @param timestampToken A TokenType object representing the timestamp format.
     * @return A TimestampFormat object containing the format string and an array indicating the order of timestamp components.
     */
    static TimestampFormat createTimestampFormat(org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType timestampToken) {
        StringBuilder javaTimeFormat = new StringBuilder();
        int[] timestampComponentsOrder = new int[TimestampComponentType.values().length];
        Arrays.fill(timestampComponentsOrder, -1);
        appendTimestampComponents(timestampToken, javaTimeFormat, timestampComponentsOrder, 0);
        return new TimestampFormat(javaTimeFormat.toString(), timestampComponentsOrder);
    }

    /**
     * Appends the details of a given token to the provided javaTimeFormat and updates the timestampComponentsOrder array to reflect the
     * order of timestamp components.
     * @param token the TokenType object representing the timestamp format
     * @param javaTimeFormat the StringBuilder to append the Java time format string
     * @param timestampComponentsOrder an array to store the order of timestamp components
     * @param nextComponentIndex the next index to use in the timestampComponentsOrder array
     * @return the number of timestamp components appended to the javaTimeFormat
     */
    private static int appendTimestampComponents(
        org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType token,
        StringBuilder javaTimeFormat,
        int[] timestampComponentsOrder,
        int nextComponentIndex
    ) {
        StringBuilder tokenJavaTimeFormat = new StringBuilder();
        TokenFormat format = token.format();
        org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType[] subTokenTypes = format.getSubTokenTypes();
        char[] delimiters = format.getSubTokenDelimiters();

        int appendedComponents = 0;
        for (int i = 0; i < subTokenTypes.length; i++) {
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subToken = subTokenTypes[i];
            TimestampComponentType componentType = subToken.getTimestampComponentType();
            if (componentType != TimestampComponentType.NA) {
                timestampComponentsOrder[componentType.getCode()] = i + nextComponentIndex;
                tokenJavaTimeFormat.append(mapSubTokenTypeToPattern(subToken));
                if (i < delimiters.length) {
                    appendDelimiter(tokenJavaTimeFormat, delimiters[i]);
                }
                appendedComponents++;
            }
        }
        javaTimeFormat.append(tokenJavaTimeFormat);
        return appendedComponents;
    }

    private static void appendDelimiter(StringBuilder builder, char delimiter) {
        // Escape characters that have special meaning in DateTimeFormatter patterns
        if ("'[]#{}T".indexOf(delimiter) != -1) {
            builder.append('\'').append(delimiter).append('\'');
        } else {
            builder.append(delimiter);
        }
    }

    private static String mapSubTokenTypeToPattern(
        org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subTokenType
    ) {
        TimestampComponentType componentType = subTokenType.getTimestampComponentType();
        if (componentType == TimestampComponentType.NA) {
            return "";
        }

        // The mapping is based on the sub-token definitions in schema.yaml
        return switch (subTokenType.name()) {
            case "YYYY" -> "yyyy"; // 4-digit year
            case "MM" -> "MM";     // 2-digit month
            case "Mon" -> "MMM";    // 3-letter month abbreviation
            case "DD" -> "dd";     // 2-digit day
            case "hh" -> "hh";     // 2-digit hour (12-hour clock - 1-12)
            case "HH" -> "HH";     // 2-digit hour (24-hour clock - 0-23)
            case "mm" -> "mm";     // 2-digit minute
            case "ss" -> "ss";     // 2-digit second
            case "ms" -> "SSS";    // 3-digit millisecond
            case "us" -> "SSSSSS"; // 6-digit microsecond
            case "AP" -> "a";      // AM/PM marker
            case "TZA", "TZOhhmm" -> "Z"; // Timezone offset like +0200 or -0500
            case "Day" -> "E";     // Day of week abbreviation
            default -> "";
        };
    }
}
