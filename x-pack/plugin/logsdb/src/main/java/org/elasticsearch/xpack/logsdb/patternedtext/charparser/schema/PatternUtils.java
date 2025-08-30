/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.OperatorType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatternUtils {
    /**
     * Parse the characters pattern into a char array.
     * Handles character ranges (e.g., a-z), character groups (e.g., abcd),
     * and escaped characters (e.g., \-).
     *
     * @param pattern the character pattern to parse (e.g., {@code [0-9]}, {@code [a-zA-Z]}, {@code [\-+0-9]})
     * @return a char array containing all characters represented by the pattern
     * @throws IllegalArgumentException if the pattern format is invalid
     */
    public static char[] parseCharacters(String pattern) {
        // Validate input pattern
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("Pattern cannot be null or empty");
        }

        pattern = pattern.trim();
        if (pattern.length() < 2 || pattern.startsWith("[") == false || pattern.endsWith("]") == false) {
            throw new IllegalArgumentException("Pattern must be enclosed in square brackets: " + pattern);
        }

        // Remove brackets
        String content = pattern.substring(1, pattern.length() - 1).trim();
        Set<Character> resultChars = new HashSet<>();

        for (int i = 0; i < content.length(); i++) {
            char currentChar = content.charAt(i);

            // Handle escape sequences
            if (currentChar == '\\' && i + 1 < content.length()) {
                // Add the escaped character directly
                resultChars.add(content.charAt(i + 1));
                i++; // Skip the escaped character
            }
            // Handle character ranges (e.g., a-z)
            else if (i + 2 < content.length() && content.charAt(i + 1) == '-') {
                char endChar = content.charAt(i + 2);

                if (currentChar > endChar) {
                    throw new IllegalArgumentException("Invalid character range: " + currentChar + "-" + endChar);
                }

                for (char c = currentChar; c <= endChar; c++) {
                    resultChars.add(c);
                }

                i += 2; // Skip the processed range
            }
            // Add individual character
            else {
                resultChars.add(currentChar);
            }
        }

        // Convert set to array
        char[] result = new char[resultChars.size()];
        int index = 0;
        for (char c : resultChars) {
            result[index++] = c;
        }

        return result;
    }

    public static String[] splitChainedAndConstraints(String constraint) {
        return splitToTwoPartsIfNeeded(constraint, OperatorType.AND);
    }

    public static String[] splitChainedOrConstraints(String constraint) {
        return splitToTwoPartsIfNeeded(constraint, OperatorType.OR);
    }

    private static String[] splitToTwoPartsIfNeeded(String constraint, OperatorType operator) {
        if (constraint == null || constraint.trim().isEmpty()) {
            throw new IllegalArgumentException("Constraint cannot be null or empty");
        }

        int orIdx = constraint.indexOf(operator.getSymbol());
        if (orIdx >= 0) {
            String[] ret = new String[2];
            ret[0] = constraint.substring(0, orIdx).trim();
            ret[1] = constraint.substring(orIdx + 2).trim();
            return ret;
        } else {
            return new String[] { constraint };
        }
    }

    /**
     * Parse the simple constraint (a single, non-chained constraint) string into a ConstraintDetails object.
     * Handles:
     * - Comparison operators: {@code >=9}, {@code <10}, {@code !=4}, {@code ==99}
     * - Range expressions: {@code 0-255}, {@code (-5)-(-2)}
     * - OR values: {@code 5|6|7|9}
     * - Length constraints: {@code {9}}
     *
     * @param constraint the constraint string without logical operators
     * @return a ConstraintDetails object containing the operator and operands
     * @throws IllegalArgumentException if the constraint format is invalid
     */
    public static ConstraintDetails parseSimpleConstraint(String constraint) {
        if (constraint == null || constraint.trim().isEmpty()) {
            throw new IllegalArgumentException("Constraint cannot be null or empty");
        }

        constraint = constraint.trim();

        // Handle length constraint {n}
        if (constraint.startsWith("{") && constraint.endsWith("}")) {
            String lengthStr = constraint.substring(1, constraint.length() - 1).trim();
            return new ConstraintDetails(OperatorType.fromSymbol("{}"), new String[] { lengthStr });
        }

        // Handle set constraints (e.g., "5|6|7|9") or map constraints (e.g., "key1=value1|key2=value2")
        if (constraint.contains(OperatorType.SET.getSymbol())) {
            boolean isMap = constraint.contains("=");
            String[] parts = constraint.split("\\|");
            String[] values;
            if (isMap) {
                values = new String[parts.length * 2];
                for (int i = 0; i < parts.length; i++) {
                    String[] keyValue = parts[i].split("=");
                    if (keyValue.length != 2) {
                        throw new IllegalArgumentException("Invalid map format: " + parts[i]);
                    }
                    values[i * 2] = removeParentheses(keyValue[0].trim());
                    values[i * 2 + 1] = removeParentheses(keyValue[1].trim());
                }
                return new ConstraintDetails(OperatorType.MAP, values);
            } else {
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = removeParentheses(parts[i].trim());
                }
                return new ConstraintDetails(OperatorType.SET, parts);
            }
        }

        // Handle comparison operators
        if (constraint.startsWith(OperatorType.GREATER_THAN_OR_EQUAL.getSymbol())
            || constraint.startsWith(OperatorType.LESS_THAN_OR_EQUAL.getSymbol())
            || constraint.startsWith(OperatorType.EQUALITY.getSymbol())
            || constraint.startsWith(OperatorType.NOT_EQUAL.getSymbol())) {
            String operator = constraint.substring(0, 2);
            String operand = removeParentheses(constraint.substring(2).trim());
            return new ConstraintDetails(OperatorType.fromSymbol(operator), new String[] { operand });
        }

        if (constraint.startsWith(OperatorType.GREATER_THAN.getSymbol()) || constraint.startsWith(OperatorType.LESS_THAN.getSymbol())) {
            String operator = constraint.substring(0, 1);
            String operand = removeParentheses(constraint.substring(1).trim());
            return new ConstraintDetails(OperatorType.fromSymbol(operator), new String[] { operand });
        }

        // Handle range format (e.g., "0-255", "(-5)-(-2)")
        int dashIdx = findRangeSeparatorIndex(constraint);
        if (dashIdx > 0) {
            String lowerBound = constraint.substring(0, dashIdx).trim();
            String upperBound = constraint.substring(dashIdx + 1).trim();
            lowerBound = removeParentheses(lowerBound);
            upperBound = removeParentheses(upperBound);
            return new ConstraintDetails(OperatorType.RANGE, new String[] { lowerBound, upperBound });
        }

        throw new IllegalArgumentException("Unrecognized constraint format: " + constraint);
    }

    /**
     * Removes parentheses from the start and end of a string.
     * Converts {@code (-N)} to {@code -N}
     */
    private static String removeParentheses(String value) {
        if (value != null && value.startsWith("(") && value.endsWith(")")) {
            return value.substring(1, value.length() - 1).trim();
        }
        return value;
    }

    /**
     * Finds the index of the range separator dash in a constraint string.
     * Handles cases with negative numbers in parentheses.
     *
     * @param constraint the constraint string to search in
     * @return the index of the range separator dash, or -1 if not found
     */
    private static int findRangeSeparatorIndex(String constraint) {
        int parenLevel = 0;
        for (int i = 0; i < constraint.length(); i++) {
            char c = constraint.charAt(i);

            if (c == '(') {
                parenLevel++;
            } else if (c == ')') {
                parenLevel--;
            } else if (c == '-' && parenLevel == 0) {
                // Ensure this is not a negative number at the start
                if (i > 0 && isOperatorChar(constraint.charAt(i - 1)) == false) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Checks if a character is part of an operator
     */
    private static boolean isOperatorChar(char c) {
        return c == '=' || c == '<' || c == '>' || c == '!';
    }

    public record ConstraintDetails(OperatorType operator, String[] operands) {}

    /**
     * Parses a token format string into a TokenFormat object.
     * NOTE: This method may modify the provided subTokenTypes list by adding new ad-hoc subToken types if unknown ones are found
     * during parsing.
     *
     * @param rawFormat The format string containing subTokens (e.g., {@code $octet.$octet.$octet.$octet} or {@code (%X{8})-$dd})
     * @param subTokenDelimiters Array of characters that can separate subTokens
     * @param subTokenTypes Map of pre-defined subToken types by name
     * @return A TokenFormat object representing the parsed format
     * @throws IllegalArgumentException if the format is invalid
     */
    public static TokenFormat parseTokenFormat(
        String rawFormat,
        char[] subTokenDelimiters,
        List<SubTokenBaseType> subTokenBaseTypes,
        List<SubTokenType> subTokenTypes
    ) {
        if (rawFormat == null || rawFormat.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty");
        }

        // Split format based on delimiters while preserving them
        List<String> parts = new ArrayList<>();
        List<Character> usedDelimiters = new ArrayList<>();
        int start = 0;

        int parenLevel = 0;
        for (int i = 0; i < rawFormat.length(); i++) {
            char c = rawFormat.charAt(i);
            if (c == '(') {
                parenLevel++;
            } else if (c == ')') {
                parenLevel--;
            }
            for (char delimiter : subTokenDelimiters) {
                if (c == delimiter && parenLevel == 0) {
                    // Add the subToken before the delimiter
                    if (i > start) {
                        parts.add(removeParentheses(rawFormat.substring(start, i)));
                    }
                    // Keep track of the used delimiter
                    usedDelimiters.add(delimiter);
                    start = i + 1;
                    break;
                }
            }
        }

        // Add the last part if there is one
        if (start < rawFormat.length()) {
            parts.add(removeParentheses(rawFormat.substring(start)));
        }

        // Convert the list of used delimiters to an array
        char[] usedDelimiterChars = new char[usedDelimiters.size()];
        for (int i = 0; i < usedDelimiters.size(); i++) {
            usedDelimiterChars[i] = usedDelimiters.get(i);
        }

        // Parse each subToken
        SubTokenType[] subTokenArray = new SubTokenType[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            String part = parts.get(i).trim();
            if (part.isEmpty()) {
                throw new IllegalArgumentException("Empty subToken in format: " + rawFormat);
            }

            if (part.startsWith("$")) {
                // Reference to a pre-defined subToken type
                String typeName = part.substring(1);
                SubTokenType subTokenType = findSubTokenTypeByName(subTokenTypes, typeName);
                if (subTokenType == null) {
                    throw new IllegalArgumentException("Unknown subToken type: " + typeName);
                }
                subTokenArray[i] = subTokenType;
            } else if (part.startsWith("%")) {
                // Ad-hoc subToken definition
                if (part.length() < 2) {
                    throw new IllegalArgumentException("Invalid ad-hoc subToken: " + part);
                }

                String subTokenName = SubTokenType.ADHOC_PREFIX + part;
                SubTokenType subTokenType = findSubTokenTypeByName(subTokenTypes, subTokenName);
                if (subTokenType == null) {
                    char baseTypeSymbol = part.charAt(1);
                    SubTokenBaseType baseType = findSubTokenBaseTypeBySymbol(subTokenBaseTypes, String.valueOf(baseTypeSymbol));
                    if (baseType == null) {
                        throw new IllegalArgumentException("Unknown base type for ad-hoc subToken: " + baseTypeSymbol);
                    }
                    String constraint = part.substring(2).trim();
                    subTokenType = new SubTokenType(subTokenName, baseType, constraint, "Ad-hoc subToken");
                    // register the new ad-hoc subToken type
                    subTokenTypes.addLast(subTokenType);
                }
                subTokenArray[i] = subTokenType;
            } else {
                throw new IllegalArgumentException("Invalid subToken format: " + part);
            }
        }

        return new TokenFormat(rawFormat, usedDelimiterChars, subTokenArray);
    }

    /**
     * Parses a multi-token format string into a list of parts, which can be either a literal string or a {@link TokenType}.
     * Handles references to token types (e.g., {@code $time}) and subToken types (e.g., {@code $Mon}).
     *
     * @param format The multi-token format string (e.g., {@code $Mon, $DD $YYYY} or {@code $datetime $TZA})
     * @param tokenTypes Map of defined token types by name
     * @param boundaryChars A set of characters that define the boundaries of a token name
     * @return A list of objects, where each object is either a {@link String} literal or a {@link TokenType}
     * @throws IllegalArgumentException if the format is invalid or contains unknown token references
     */
    public static List<Object> parseMultiTokenFormat(String format, List<TokenType> tokenTypes, Set<Character> boundaryChars) {
        if (format == null || format.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty");
        }

        List<Object> parts = new ArrayList<>();
        StringBuilder currentPart = new StringBuilder();
        boolean isTokenNamePart = false;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (isTokenNamePart) {
                if (c == '$') {
                    throw new IllegalArgumentException("Token names must be separated by delimiters: " + format);
                } else {
                    if (boundaryChars.contains(c)) {
                        // end of token name
                        addMultiTokenPart(format, tokenTypes, currentPart, isTokenNamePart, parts);
                        currentPart.setLength(0); // reset for next token
                        isTokenNamePart = false;
                    }
                    currentPart.append(c);
                }
            } else {
                if (c == '$') {
                    addMultiTokenPart(format, tokenTypes, currentPart, isTokenNamePart, parts);
                    currentPart.setLength(0);
                    isTokenNamePart = true;
                } else if (boundaryChars.contains(c)) {
                    currentPart.append(c);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid format - only token delimiters and trimmed characters are allowed between tokens: " + format
                    );
                }
            }
        }
        addMultiTokenPart(format, tokenTypes, currentPart, isTokenNamePart, parts);
        return parts;
    }

    private static void addMultiTokenPart(
        String format,
        List<TokenType> tokenTypes,
        StringBuilder currentPart,
        boolean isTokenNamePart,
        List<Object> parts
    ) {
        // noinspection SizeReplaceableByIsEmpty
        if (currentPart.length() > 0) {
            if (isTokenNamePart) {
                // if the last part is a token name, it must end with a boundary character
                String tokenName = currentPart.toString();
                TokenType token = findTokenTypeByName(tokenTypes, tokenName);
                if (token == null) {
                    throw new IllegalArgumentException("Unknown token type: " + tokenName + " in format: " + format);
                }
                parts.add(token);
            } else {
                // otherwise, it's a literal part
                parts.add(currentPart.toString());
            }
        } else if (isTokenNamePart) {
            throw new IllegalArgumentException("Token name cannot be empty in format: " + format);
        }
    }

    private static SubTokenType findSubTokenTypeByName(List<SubTokenType> list, String name) {
        for (SubTokenType t : list) {
            if (t.name().equals(name)) return t;
        }
        return null;
    }

    private static SubTokenBaseType findSubTokenBaseTypeBySymbol(List<SubTokenBaseType> list, String symbol) {
        for (SubTokenBaseType t : list) {
            if (t.symbol().equals(symbol)) return t;
        }
        return null;
    }

    private static TokenType findTokenTypeByName(List<TokenType> list, String name) {
        for (TokenType t : list) {
            if (t.name().equals(name)) return t;
        }
        return null;
    }
}
