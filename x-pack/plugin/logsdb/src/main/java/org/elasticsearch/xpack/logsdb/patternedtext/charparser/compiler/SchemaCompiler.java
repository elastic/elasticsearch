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
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.CharSpecificParsingInfo;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.MultiTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringToIntegerMap;
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
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringToIntMapConstraint;

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
import java.util.function.ToIntFunction;

import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.ALPHABETIC_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.DIGIT_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.OTHER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_BOUNDARY_CHAR_CODE;
import static org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes.TOKEN_DELIMITER_CHAR_CODE;

public class SchemaCompiler {

    public static final int ASCII_RANGE = 128;
    public static final int SMALL_INTEGERS_MAX_VALUE = 100;
    public static final String INTEGER_SUBTOKEN_NAME = "integer";
    public static final String DOUBLE_SUBTOKEN_NAME = "double";
    public static final String HEX_SUBTOKEN_NAME = "hex";

    // todo - try to break this method into smaller methods
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
        // for each delimiter char, we store a list of functions that can generate a token bitmask based on the string value of the
        // subToken, the subToken index, and the delimiter char
        Map<Character, ArrayList<SubstringToBitmaskChain.Builder>> delimiterCharToBitmaskGeneratorPerSubTokenIndex = new HashMap<>();
        // for each token format, the last subToken is not identified by a subToken delimiter, but rather by a token delimiter
        ArrayList<SubstringToBitmaskChain.Builder> bitmaskGeneratorForLastSubToken = new ArrayList<>();
        // a global map for all string subToken types, that maps a string value to the corresponding subToken bitmask
        SubstringToIntegerMap.Builder subTokenValueToBitmaskMapBuilder = SubstringToIntegerMap.builder();
        // for each token boundary character (either token delimiters or boundary characters), we store the superset of multi-token types
        // that are valid for this character at each index within the total concatenated delimiter parts of all multi-token formats.
        // For example, given the multi-token format "$Mon, $DD $YYYY $timeS $AP", the full concatenated string made up of the delimiter
        // parts is ", ". Therefore, the bit of this multi-token will be set at index 0 for the character ',' and at indices
        // 1, 2, 3, and 4 for the space character.
        Map<Character, ArrayList<Integer>> tokenBoundaryCharToMultiTokenBitmaskPerIndex = new HashMap<>();

        int allSubTokenBitmask = 0;
        int intSubTokenBitmask;
        int allIntegerSubTokenBitmask = 0;
        int genericSubTokenTypesBitmask = 0;

        Map<Integer, Integer> subTokenCountToTokenBitmaskMap = new HashMap<>();
        Map<Integer, Integer> tokenCountToMultiTokenBitmaskMap = new HashMap<>();
        Map<Integer, Integer> subTokenCountToMultiTokenBitmaskMap = new HashMap<>();

        int maxTokensPerMultiToken = 0;
        int maxSubTokensPerMultiToken = 0;
        int maxDelimiterPartsLength = 0;

        Map<String, ArrayList<Integer>> tokenTypeToMultiTokenBitmaskByPosition = new HashMap<>();
        BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry = new BitmaskRegistry<>();
        ArrayList<Integer> multiTokenBitmaskPerDelimiterPartsLengths = new ArrayList<>();
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

            updateBitmaskToCount(tokenCountToMultiTokenBitmaskMap, tokens.size(), multiTokenBitmask);
            updateBitmaskToCount(subTokenCountToMultiTokenBitmaskMap, subTokenCount, multiTokenBitmask);

            for (int i = 0; i < tokens.size(); i++) {
                String tokenName = tokens.get(i).name();
                updateBitmaskByPosition(tokenTypeToMultiTokenBitmaskByPosition, tokenName, i, multiTokenBitmask);
            }

            int delimiterCharPosition = -1;
            for (String delimiterPart : format.getDelimiterParts()) {
                for (char tokenBoundaryCharacter : delimiterPart.toCharArray()) {
                    delimiterCharPosition++;
                    ArrayList<Integer> multiTokenBitmaskPerDelimiterIndex = tokenBoundaryCharToMultiTokenBitmaskPerIndex.computeIfAbsent(
                        tokenBoundaryCharacter,
                        input -> new ArrayList<>()
                    );
                    fillListUpToIndex(multiTokenBitmaskPerDelimiterIndex, delimiterCharPosition, () -> 0);
                    multiTokenBitmaskPerDelimiterIndex.set(
                        delimiterCharPosition,
                        multiTokenBitmaskPerDelimiterIndex.get(delimiterCharPosition) | multiTokenBitmask
                    );
                }
            }
            maxDelimiterPartsLength = Math.max(maxDelimiterPartsLength, delimiterCharPosition + 1);
            fillListUpToIndex(multiTokenBitmaskPerDelimiterPartsLengths, delimiterCharPosition, () -> 0);
            multiTokenBitmaskPerDelimiterPartsLengths.set(
                delimiterCharPosition,
                multiTokenBitmaskPerDelimiterPartsLengths.get(delimiterCharPosition) | multiTokenBitmask
            );
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

        // the double generic token type can span unknown number of tokens (e.g., "3.14" or "-1.06-e10")
        int doubleTokenBitmask = tokenBitmaskRegistry.register(
            new TokenType(DOUBLE_SUBTOKEN_NAME, EncodingType.DOUBLE, -1, null, new int[maxTokensPerMultiToken])
        );
        // floating point numbers can have 2 or 3 sub-tokens (e.g. "3.14" has 2 sub-tokens, "-1.06-e10" has 3 sub-tokens)
        updateBitmaskToCount(subTokenCountToTokenBitmaskMap, 2, doubleTokenBitmask);
        updateBitmaskToCount(subTokenCountToTokenBitmaskMap, 3, doubleTokenBitmask);

        tokenBitmaskRegistry.register(new TokenType(HEX_SUBTOKEN_NAME, EncodingType.HEX, 1, null, new int[maxTokensPerMultiToken]));
        tokenBitmaskRegistry.register(new TokenType(INTEGER_SUBTOKEN_NAME, EncodingType.INTEGER, 1, null, new int[maxTokensPerMultiToken]));

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

            updateBitmaskToCount(subTokenCountToTokenBitmaskMap, subTokenCount, tokenBitmask);

            maxSubTokensPerToken = Math.max(maxSubTokensPerToken, subTokenCount);
            TokenFormat format = tokenType.format();
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType[] subTokenTypes = format.getSubTokenTypes();
            for (int i = 0; i < subTokenTypes.length; i++) {
                org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subTokenType = subTokenTypes[i];
                String subTokenName = subTokenType.name();
                updateBitmaskByPosition(subTokenTypeToTokenBitmaskByPosition, subTokenName, i, tokenBitmask);

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

        ArrayList<SubTokenBaseType> subTokenBaseTypes = schema.getSubTokenBaseTypes();

        // Register generic subToken types first, as they would have the lowest priority

        int[] doubleTokenBitmaskByPosition = new int[maxSubTokensPerToken];
        doubleTokenBitmaskByPosition[0] = doubleTokenBitmask;
        doubleTokenBitmaskByPosition[1] = doubleTokenBitmask;
        doubleTokenBitmaskByPosition[2] = doubleTokenBitmask;
        int doubleSubTokenBitmask = subTokenBitmaskRegistry.register(
            new SubTokenType(DOUBLE_SUBTOKEN_NAME, EncodingType.DOUBLE, doubleTokenBitmaskByPosition, TimestampComponentType.NA)
        );
        genericSubTokenTypesBitmask |= doubleSubTokenBitmask;
        allSubTokenBitmask |= doubleSubTokenBitmask;
        SubTokenBaseType doubleSubTokenBaseType = subTokenBaseTypes.stream()
            .filter(baseType -> baseType.name().equals("double"))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Double subToken base type not found in schema"));
        updateCharToSubTokenBitmasks(
            DOUBLE_SUBTOKEN_NAME,
            charToSubTokenBitmask,
            doubleSubTokenBaseType.allowedCharacters(),
            doubleSubTokenBitmask
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

        intSubTokenBitmask = subTokenBitmaskRegistry.register(
            new SubTokenType(INTEGER_SUBTOKEN_NAME, EncodingType.INTEGER, new int[maxSubTokensPerToken], TimestampComponentType.NA)
        );
        allIntegerSubTokenBitmask |= intSubTokenBitmask;
        genericSubTokenTypesBitmask |= intSubTokenBitmask;
        allSubTokenBitmask |= intSubTokenBitmask;
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

        int[] smallIntegerSubTokenBitmasks = new int[SMALL_INTEGERS_MAX_VALUE + 1];
        for (int i = 0; i <= SMALL_INTEGERS_MAX_VALUE; i++) {
            smallIntegerSubTokenBitmasks[i] = genericSubTokenTypesBitmask;
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
                Map<Character, Set<Integer>> delimiterCharToPositions = subTokenTypeToDelimiterCharToPositions.get(subTokenType);
                if (delimiterCharToPositions != null) {
                    for (Map.Entry<Character, Set<Integer>> entry : delimiterCharToPositions.entrySet()) {
                        char subTokenDelimiter = entry.getKey();
                        ArrayList<SubstringToBitmaskChain.Builder> bitmaskGeneratorPerSubTokenIndex =
                            delimiterCharToBitmaskGeneratorPerSubTokenIndex.computeIfAbsent(subTokenDelimiter, input -> new ArrayList<>());
                        Set<Integer> positions = entry.getValue();
                        fillListUpToIndex(bitmaskGeneratorPerSubTokenIndex, Collections.max(positions), () -> null);
                        addConstraintToChain(bitmaskGeneratorPerSubTokenIndex, subTokenBitmask, stringConstraint, positions);
                    }
                }
                // for the last subToken, we use the token delimiter to identify it, so we add it to the subTokenEvaluatorForLastSubToken
                Set<Integer> lastPositionIndices = subTokenTypeToLastSubTokenIndex.get(subTokenType);
                if (lastPositionIndices != null) {
                    fillListUpToIndex(bitmaskGeneratorForLastSubToken, Collections.max(lastPositionIndices), () -> null);
                    addConstraintToChain(bitmaskGeneratorForLastSubToken, subTokenBitmask, stringConstraint, lastPositionIndices);
                }

                // add mapping for string subTokens that map specific string values to specific numeric values
                if (stringConstraint instanceof StringToIntMapConstraint(Map<String, Integer> map)) {
                    map.forEach((key, value) -> {
                        Integer existing = subTokenValueToBitmaskMapBuilder.get(key);
                        if (existing != null) {
                            throw new IllegalArgumentException(
                                "SubToken value '"
                                    + key
                                    + "' is mapped to multiple numeric values: "
                                    + existing
                                    + " and "
                                    + value
                                    + ". Each subToken value can only be mapped to a single bitmask."
                            );
                        } else {
                            subTokenValueToBitmaskMapBuilder.add(key, value);
                        }
                    });
                }
            }
        }
        subTokenBitmaskRegistry.seal();

        // taking care of delimiter characters related to floating point numbers like '.' and '-'
        ArrayList<Integer> dashBitmaskPerSubTokenCount = delimiterCharToTokenBitmaskPerSubTokenIndex.computeIfAbsent(
            '-',
            input -> new ArrayList<>()
        );
        fillListUpToIndex(dashBitmaskPerSubTokenCount, 1, () -> 0);
        dashBitmaskPerSubTokenCount.set(0, dashBitmaskPerSubTokenCount.get(0) | doubleSubTokenBitmask);
        dashBitmaskPerSubTokenCount.set(1, dashBitmaskPerSubTokenCount.get(1) | doubleSubTokenBitmask);
        ArrayList<Integer> dotBitmaskPerSubTokenCount = delimiterCharToTokenBitmaskPerSubTokenIndex.computeIfAbsent(
            '.',
            input -> new ArrayList<>()
        );
        fillListUpToIndex(dotBitmaskPerSubTokenCount, 0, () -> 0);
        dotBitmaskPerSubTokenCount.set(0, dotBitmaskPerSubTokenCount.getFirst() | doubleSubTokenBitmask);

        // update floating point bitmask also for last sub-token for 1, 2, and 3 sub-tokens
        tokenBitmaskForLastSubToken.set(0, tokenBitmaskForLastSubToken.get(0) | doubleSubTokenBitmask);
        tokenBitmaskForLastSubToken.set(1, tokenBitmaskForLastSubToken.get(1) | doubleSubTokenBitmask);
        tokenBitmaskForLastSubToken.set(2, tokenBitmaskForLastSubToken.get(2) | doubleSubTokenBitmask);

        CharSpecificParsingInfo[] charSpecificParsingInfos = new CharSpecificParsingInfo[ASCII_RANGE];

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
                ArrayList<SubstringToBitmaskChain.Builder> subTokenBitmaskGeneratorPerSubTokenIndexList =
                    delimiterCharToBitmaskGeneratorPerSubTokenIndex.get(delimiter);
                ToIntFunction<SubstringView>[] subTokenBitmaskGeneratorPerSubTokenIndices = turnChainBuilderListToFunctionArray(
                    subTokenBitmaskGeneratorPerSubTokenIndexList,
                    maxSubTokensPerToken
                );
                charSpecificParsingInfos[delimiter] = new CharSpecificParsingInfo(
                    delimiter,
                    tokenBitmaskPerSubTokenIndex,
                    subTokenBitmaskGeneratorPerSubTokenIndices,
                    null
                );
            } else {
                throw new IllegalArgumentException(
                    "SubToken delimiter character '" + delimiter + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        for (char tokenBoundaryCharacter : schema.getTokenBoundaryCharacters()) {
            if (tokenBoundaryCharacter < ASCII_RANGE) {
                int[] multiTokenBitmaskPerBoundaryCharIndex = getMultiTokenBitmaskPerBoundaryCharIndex(
                    tokenBoundaryCharacter,
                    tokenBoundaryCharToMultiTokenBitmaskPerIndex,
                    maxDelimiterPartsLength
                );
                CharSpecificParsingInfo tokenBoundaryCharParsingInfo = new CharSpecificParsingInfo(
                    tokenBoundaryCharacter,
                    null,
                    null,
                    multiTokenBitmaskPerBoundaryCharIndex
                );
                charSpecificParsingInfos[tokenBoundaryCharacter] = tokenBoundaryCharParsingInfo;
            } else {
                throw new IllegalArgumentException(
                    "Token boundary character '" + tokenBoundaryCharacter + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        int[] tokenBitmaskPerSubTokenIndex = tokenBitmaskForLastSubToken.stream().mapToInt(Integer::intValue).toArray();
        ToIntFunction<SubstringView>[] subTokenBitmaskGeneratorForLastIndex = turnChainBuilderListToFunctionArray(
            bitmaskGeneratorForLastSubToken,
            maxSubTokensPerToken
        );
        for (char delimiter : schema.getTokenDelimiters()) {
            if (delimiter < ASCII_RANGE) {
                // token delimiter characters are also subToken delimiters (that delimit between the last subToken to the next token)
                // and therefore should be mapped to the inclusive subToken bitmask, as they are valid to all subToken types
                charToSubTokenBitmask[delimiter] = allSubTokenBitmask;

                int[] multiTokenBitmaskPerBoundaryCharIndex = getMultiTokenBitmaskPerBoundaryCharIndex(
                    delimiter,
                    tokenBoundaryCharToMultiTokenBitmaskPerIndex,
                    maxDelimiterPartsLength
                );
                CharSpecificParsingInfo tokenDelimiterCharParsingInfo = new CharSpecificParsingInfo(
                    delimiter,
                    tokenBitmaskPerSubTokenIndex,
                    subTokenBitmaskGeneratorForLastIndex,
                    multiTokenBitmaskPerBoundaryCharIndex
                );
                charSpecificParsingInfos[delimiter] = tokenDelimiterCharParsingInfo;
            } else {
                throw new IllegalArgumentException(
                    "Token delimiter character '" + delimiter + "' is outside the ASCII range and will not be processed."
                );
            }
        }

        for (char tokenBoundaryChar : schema.getTokenBoundaryCharacters()) {
            if (tokenBoundaryChar < ASCII_RANGE) {
                // token boundary characters should not invalidate any sub-token, so we use the inclusive sub-token bitmask
                charToSubTokenBitmask[tokenBoundaryChar] = allSubTokenBitmask;
            } else {
                throw new IllegalArgumentException(
                    "Token boundary character '" + tokenBoundaryChar + "' is outside the ASCII range and will not be processed."
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

        int[] delimiterLengthToMultiTokenBitmask = new int[maxDelimiterPartsLength];
        for (int i = 0; i < multiTokenBitmaskPerDelimiterPartsLengths.size(); i++) {
            delimiterLengthToMultiTokenBitmask[i] = multiTokenBitmaskPerDelimiterPartsLengths.get(i);
        }

        intRangeBitmasks = mergeIntRangeBitmasks(intRangeBitmasks);
        int[] integerSubTokenBitmaskArrayRanges = new int[intRangeBitmasks.size()];
        int[] integerSubTokenBitmasks = new int[intRangeBitmasks.size()];
        for (int i = 0; i < intRangeBitmasks.size(); i++) {
            IntRangeBitmask intRangeBitmask = intRangeBitmasks.get(i);
            integerSubTokenBitmaskArrayRanges[i] = intRangeBitmask.range().upperBound();
            // ensure that the generic subToken (int, double and hex) bits are always set, as they apply to all integer values
            integerSubTokenBitmasks[i] = intRangeBitmask.bitmask() | genericSubTokenTypesBitmask;
        }

        return new CompiledSchema(
            charToSubTokenBitmask,
            charToCharType,
            charSpecificParsingInfos,
            subTokenValueToBitmaskMapBuilder.build(),
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
            delimiterLengthToMultiTokenBitmask,
            subTokenBitmaskRegistry,
            tokenBitmaskRegistry,
            multiTokenBitmaskRegistry
        );
    }

    private static void updateBitmaskByPosition(
        Map<String, ArrayList<Integer>> typeToHigherTypeBitmaskByPosition,
        String lowerTypeName,
        int i,
        int tokenBitmask
    ) {
        ArrayList<Integer> bitmaskList = typeToHigherTypeBitmaskByPosition.computeIfAbsent(lowerTypeName, input -> new ArrayList<>());
        fillListUpToIndex(bitmaskList, i, () -> 0);
        bitmaskList.set(i, bitmaskList.get(i) | tokenBitmask);
    }

    private static void updateBitmaskToCount(Map<Integer, Integer> typeCountToHigherTypeBitmask, int subTokenCount, int tokenBitmask) {
        int bitmaskForSubTokenCount = typeCountToHigherTypeBitmask.computeIfAbsent(subTokenCount, input -> 0);
        bitmaskForSubTokenCount |= tokenBitmask;
        typeCountToHigherTypeBitmask.put(subTokenCount, bitmaskForSubTokenCount);
    }

    private static int[] getMultiTokenBitmaskPerBoundaryCharIndex(
        char tokenBoundaryCharacter,
        Map<Character, ArrayList<Integer>> tokenBoundaryCharToMultiTokenBitmaskPerIndex,
        int maxDelimiterPartsLength
    ) {
        int[] multiTokenBitmaskPerBoundaryCharIndex = null;
        ArrayList<Integer> multiTokenBitmaskPerBoundaryCharIndexArray = tokenBoundaryCharToMultiTokenBitmaskPerIndex.get(
            tokenBoundaryCharacter
        );
        if (multiTokenBitmaskPerBoundaryCharIndexArray != null) {
            multiTokenBitmaskPerBoundaryCharIndex = new int[maxDelimiterPartsLength];
            fillListUpToIndex(multiTokenBitmaskPerBoundaryCharIndexArray, maxDelimiterPartsLength - 1, () -> 0);
            for (int i = 0; i < multiTokenBitmaskPerBoundaryCharIndexArray.size(); i++) {
                multiTokenBitmaskPerBoundaryCharIndex[i] = multiTokenBitmaskPerBoundaryCharIndexArray.get(i);
            }
        }
        return multiTokenBitmaskPerBoundaryCharIndex;
    }

    /**
     * Adds a string constraint to the all chains at the specified positions.
     * The addition step actually includes the compilation of the constraint into a runtime evaluation function.
     * @param chainForIndex the list of chains, one per subToken index
     * @param subTokenBitmask the subToken bitmask associated with the constraint
     * @param stringConstraint the string constraint to compile and add
     * @param positionIndices the delimiter positions within the subToken type where the constraint should be added
     */
    private static void addConstraintToChain(
        ArrayList<SubstringToBitmaskChain.Builder> chainForIndex,
        int subTokenBitmask,
        StringConstraint stringConstraint,
        Set<Integer> positionIndices
    ) {
        positionIndices.forEach(position -> {
            SubstringToBitmaskChain.Builder chainBuilder = chainForIndex.get(position);
            if (chainBuilder == null) {
                chainBuilder = SubstringToBitmaskChain.builder();
                chainForIndex.set(position, chainBuilder);
            }
            chainBuilder.add(stringConstraint, subTokenBitmask);
        });
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

        for (char tokenBoundaryChar : schema.getTokenBoundaryCharacters()) {
            if (c == tokenBoundaryChar) {
                return TOKEN_BOUNDARY_CHAR_CODE;
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

    @SuppressWarnings("unchecked")
    private static ToIntFunction<SubstringView>[] turnChainBuilderListToFunctionArray(
        ArrayList<SubstringToBitmaskChain.Builder> buildersList,
        int maxSubTokensPerToken
    ) {
        if (buildersList != null) {
            ArrayList<ToIntFunction<SubstringView>> tmpList = new ArrayList<>();
            fillListUpToIndex(tmpList, maxSubTokensPerToken - 1, () -> null);
            for (int i = 0; i < buildersList.size(); i++) {
                SubstringToBitmaskChain.Builder chainBuilder = buildersList.get(i);
                if (chainBuilder != null) {
                    tmpList.set(i, chainBuilder.build());
                }
            }
            return tmpList.toArray(ToIntFunction[]::new);
        }
        return null;
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

        @SuppressWarnings("NullableProblems")
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
        List<String> delimiterParts = format.getDelimiterParts();
        List<org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType> tokens = format.getTokens();
        if (delimiterParts.size() != tokens.size() - 1) {
            throw new IllegalArgumentException(
                "Invalid MultiTokenFormat: number of delimiter parts ("
                    + delimiterParts.size()
                    + ") must be one less than number of tokens ("
                    + tokens.size()
                    + ")."
            );
        }

        boolean isUsingAmPm = tokens.stream()
            .anyMatch(token -> Arrays.stream(token.format().getSubTokenTypes()).anyMatch(subToken -> subToken.name().equals("AP")));

        int[] timestampComponentsOrder = new int[TimestampComponentType.values().length];
        Arrays.fill(timestampComponentsOrder, -1);
        int nextComponentIndex = 0;
        StringBuilder javaTimeFormat = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.TokenType token = tokens.get(i);
            StringBuilder tokenJavaTimeFormat = new StringBuilder();
            nextComponentIndex += appendTimestampComponents(
                token,
                tokenJavaTimeFormat,
                timestampComponentsOrder,
                nextComponentIndex,
                isUsingAmPm
            );
            javaTimeFormat.append(tokenJavaTimeFormat);
            if (i < delimiterParts.size()) {
                String delimiterPart = delimiterParts.get(i);
                for (char c : delimiterPart.toCharArray()) {
                    appendDelimiter(javaTimeFormat, c);
                }
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
        boolean isUsingAmPm = Arrays.stream(timestampToken.format().getSubTokenTypes()).anyMatch(subToken -> subToken.name().equals("AP"));
        appendTimestampComponents(timestampToken, javaTimeFormat, timestampComponentsOrder, 0, isUsingAmPm);
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
        int nextComponentIndex,
        boolean isUsingAmPm
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
                tokenJavaTimeFormat.append(mapSubTokenTypeToPattern(subToken, isUsingAmPm));
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
        org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType subTokenType,
        boolean useAmPm
    ) {
        TimestampComponentType componentType = subTokenType.getTimestampComponentType();
        if (componentType == TimestampComponentType.NA) {
            return "";
        }

        // The mapping is based on the sub-token definitions in schema.yaml
        return switch (subTokenType.name()) {
            case "YYYY" -> "yyyy";                  // 4-digit year
            case "MM" -> "MM";                      // 2-digit month
            case "Mon" -> "MMM";                    // 3-letter month abbreviation
            case "DD" -> "dd";                      // 2-digit day
            case "hh" -> useAmPm ? "hh" : "HH";     // 2-digit hour (either 12-hour or 24-hour clock - 0-23)
            case "mm" -> "mm";                      // 2-digit minute
            case "ss" -> "ss";                      // 2-digit second
            case "ms" -> "SSS";                     // 3-digit millisecond
            case "us" -> "SSSSSS";                  // 6-digit microsecond
            case "AP" -> "a";                       // AM/PM marker
            case "TZA", "TZOhhmm" -> "Z";           // Timezone offset like +0200 or -0500
            case "Day" -> "E";                      // Day of week abbreviation
            default -> "";
        };
    }
}
