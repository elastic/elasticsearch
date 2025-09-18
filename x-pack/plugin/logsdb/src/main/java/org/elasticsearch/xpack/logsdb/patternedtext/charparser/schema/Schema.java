/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.Type;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Loads and parses the schema.yaml file for token and sub-token definitions.
 */
@SuppressWarnings({ "unchecked", "DataFlowIssue" })
public class Schema {
    private static final String SCHEMA_PATH = "/schema.yaml";

    private static volatile Schema instance;

    private final char[] tokenDelimiters;
    private final char[] subTokenDelimiters;
    private final char[] trimmedCharacters;
    private final ArrayList<SubTokenBaseType> subTokenBaseTypes;
    private final ArrayList<SubTokenType> subTokenTypes;
    private final ArrayList<TokenType> tokenTypes;
    private final ArrayList<MultiTokenType> multiTokenTypes;

    public static Schema getInstance() {
        if (instance == null) {
            synchronized (Schema.class) {
                if (instance == null) {
                    instance = new Schema();
                }
            }
        }
        return instance;
    }

    private Schema() {
        try {
            Map<String, Object> yamlMap = readYamlFileToMap();

            // Parse special characters
            Map<String, Object> specialCharsMap = (Map<String, Object>) yamlMap.get("special_characters");
            // we should not trim the token delimiters, as they contain whitespaces
            tokenDelimiters = ((String) specialCharsMap.get("token_delimiters")).toCharArray();
            subTokenDelimiters = getConfigValue(specialCharsMap, "sub_token_delimiters").toCharArray();
            trimmedCharacters = getConfigValue(specialCharsMap, "trimmed_characters").toCharArray();

            Map<String, Object> schemaMap = (Map<String, Object>) yamlMap.get("schema");
            subTokenBaseTypes = parseSubTokenBaseTypes((List<Object>) schemaMap.get("sub_token_base_types"));
            subTokenTypes = parseSubTokenTypes((List<Object>) schemaMap.get("sub_token_types"));
            tokenTypes = parseTokenTypes((List<Object>) schemaMap.get("token_types"));
            multiTokenTypes = parseMultiTokenTypes((List<Object>) schemaMap.get("multi_token_types"));

        } catch (Exception e) {
            throw new RuntimeException("Error loading schema", e);
        }
    }

    private ArrayList<SubTokenBaseType> parseSubTokenBaseTypes(List<Object> subTokenBaseTypeConfigs) {
        ArrayList<SubTokenBaseType> subTokenBaseTypes = new ArrayList<>();
        for (Object obj : subTokenBaseTypeConfigs) {
            Map<String, Object> config = (Map<String, Object>) obj;
            String name = getConfigValue(config, "name");
            String symbol = getConfigValue(config, "symbol");
            String encodingTypeStr = getConfigValue(config, "encoding_type");
            String javaTypeStr = getConfigValue(config, "java_type");
            String charactersPattern = getConfigValue(config, "characters");
            String description = getConfigValue(config, "description");

            EncodingType encodingType = EncodingType.fromSymbol(encodingTypeStr.charAt(1));

            // Convert java_type string to actual Class object
            Class<?> javaType = switch (javaTypeStr) {
                case "int" -> int.class;
                case "double" -> double.class;
                case "String" -> String.class;
                default -> throw new IllegalArgumentException("Unsupported Java type: " + javaTypeStr);
            };

            char[] allowedCharacters = PatternUtils.parseCharacters(charactersPattern);
            subTokenBaseTypes.add(new SubTokenBaseType(name, encodingType, symbol, javaType, description, allowedCharacters));
        }
        return subTokenBaseTypes;
    }

    private ArrayList<SubTokenType> parseSubTokenTypes(List<Object> subTokenTypeConfigs) {
        ArrayList<SubTokenType> result = new ArrayList<>();
        for (Object obj : subTokenTypeConfigs) {
            Map<String, Object> config = (Map<String, Object>) obj;
            String name = getConfigValue(config, "name");
            String baseTypeStr = getConfigValue(config, "base_type");
            // Remove the '%' prefix
            SubTokenBaseType baseType = fromSymbol(baseTypeStr.substring(1));

            String constraint = getConfigValue(config, "constraint");
            String timestampComponentTypeStr = getConfigValue(config, "timestamp_component_type");
            TimestampComponentType timestampComponentType;
            if (timestampComponentTypeStr == null) {
                timestampComponentType = TimestampComponentType.NA;
            } else {
                timestampComponentType = TimestampComponentType.fromSymbol(timestampComponentTypeStr);
            }
            String description = getConfigValue(config, "description");
            result.addLast(new SubTokenType(name, baseType, constraint, description, timestampComponentType));
        }
        return result;
    }

    private ArrayList<TokenType> parseTokenTypes(List<Object> tokenTypesList) {
        ArrayList<TokenType> result = new ArrayList<>();
        for (Object obj : tokenTypesList) {
            Map<String, Object> typeMap = (Map<String, Object>) obj;
            String name = getConfigValue(typeMap, "name");
            String encodingTypeStr = getConfigValue(typeMap, "encoding_type");
            // Remove the '%' prefix
            EncodingType encodingType = EncodingType.fromSymbol(encodingTypeStr.charAt(1));

            String specialSubTokenDelimitersRaw = getConfigValue(typeMap, "special_sub_token_delimiters");
            char[] actualSubTokenDelimiters = combineCharArrays(specialSubTokenDelimitersRaw, subTokenDelimiters);

            String rawFormat = (String) typeMap.get("format");
            TokenFormat format = PatternUtils.parseTokenFormat(rawFormat, actualSubTokenDelimiters, subTokenBaseTypes, subTokenTypes);

            String description = getConfigValue(typeMap, "description");
            result.addLast(new TokenType(name, encodingType, format, description));
        }

        return result;
    }

    private char[] combineCharArrays(String specialSubTokenDelimitersConfig, char[] baseDelimiters) {
        char[] actualDelimiters;
        if (specialSubTokenDelimitersConfig != null) {
            char[] specialSubTokenDelimiters = specialSubTokenDelimitersConfig.toCharArray();
            actualDelimiters = new char[baseDelimiters.length + specialSubTokenDelimiters.length];
            System.arraycopy(baseDelimiters, 0, actualDelimiters, 0, baseDelimiters.length);
            System.arraycopy(specialSubTokenDelimiters, 0, actualDelimiters, baseDelimiters.length, specialSubTokenDelimiters.length);
        } else {
            actualDelimiters = baseDelimiters;
        }
        return actualDelimiters;
    }

    private ArrayList<MultiTokenType> parseMultiTokenTypes(List<Object> multiTokenTypesList) {
        ArrayList<MultiTokenType> result = new ArrayList<>();
        // Create the set of boundary characters once
        Set<Character> boundaryChars = getTokenBoundaryChars();

        for (Object obj : multiTokenTypesList) {
            Map<String, Object> typeMap = (Map<String, Object>) obj;
            String name = getConfigValue(typeMap, "name");
            String encodingTypeStr = getConfigValue(typeMap, "encoding_type");
            // Remove the '%' prefix
            EncodingType encodingType = EncodingType.fromSymbol(encodingTypeStr.charAt(1));

            String rawFormat = (String) typeMap.get("format");
            List<Object> formatParts = PatternUtils.parseMultiTokenFormat(rawFormat, tokenTypes, boundaryChars);
            MultiTokenFormat format = new MultiTokenFormat(rawFormat, formatParts);

            String description = getConfigValue(typeMap, "description");
            result.addLast(new MultiTokenType(name, encodingType, format, description));
        }

        return result;
    }

    public Set<Character> getTokenBoundaryChars() {
        Set<Character> boundaryChars = new HashSet<>();
        for (char c : getTokenDelimiters()) {
            boundaryChars.add(c);
        }
        for (char c : getTrimmedCharacters()) {
            boundaryChars.add(c);
        }
        return boundaryChars;
    }

    public char[] getTokenDelimiters() {
        return tokenDelimiters;
    }

    public char[] getSubTokenDelimiters() {
        return subTokenDelimiters;
    }

    public char[] getTrimmedCharacters() {
        return trimmedCharacters;
    }

    public ArrayList<SubTokenBaseType> getSubTokenBaseTypes() {
        return subTokenBaseTypes;
    }

    public ArrayList<SubTokenType> getSubTokenTypes() {
        return subTokenTypes;
    }

    public ArrayList<TokenType> getTokenTypes() {
        return tokenTypes;
    }

    public ArrayList<MultiTokenType> getMultiTokenTypes() {
        return multiTokenTypes;
    }

    public SubTokenBaseType fromSymbol(String symbol) {
        for (SubTokenBaseType baseType : subTokenBaseTypes) {
            if (baseType.symbol().equals(symbol)) {
                return baseType;
            }
        }
        return null;
    }

    public SubTokenType getSubTokenType(String name) {
        return getTypeByName(subTokenTypes, name);
    }

    public TokenType getTokenType(String name) {
        return getTypeByName(tokenTypes, name);
    }

    public MultiTokenType getMultiTokenType(String name) {
        return getTypeByName(multiTokenTypes, name);
    }

    public static <T extends Type> T getTypeByName(List<T> types, String name) {
        for (T type : types) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Schema {\n");

        // Special Characters
        sb.append("  Special Characters:\n");
        sb.append("    TokenDelimiters: [");
        printCharArray(tokenDelimiters, sb);
        sb.append("]\n");

        sb.append("    SubTokenDelimiters: [");
        printCharArray(subTokenDelimiters, sb);
        sb.append("]\n");

        sb.append("    TrimmedCharacters: [");
        printCharArray(trimmedCharacters, sb);
        sb.append("]\n\n");

        // SubTokenBaseTypes
        sb.append("  SubTokenBaseTypes {\n");
        subTokenBaseTypes.forEach(
            baseType -> sb.append("    ")
                .append(baseType.name())
                .append(" {\n")
                .append("      symbol: ")
                .append(baseType.symbol())
                .append("\n")
                .append("      encodingType: ")
                .append(baseType.encodingType().name())
                .append("\n")
                .append("      baseType: ")
                .append(baseType.baseType().getSimpleName())
                .append("\n")
                .append("      description: ")
                .append(baseType.description())
                .append("\n")
                .append("    }\n")
        );
        sb.append("  }\n\n");

        // SubTokenTypes
        sb.append("  SubTokenTypes {\n");
        subTokenTypes.forEach(subTokenType -> {
            sb.append("    ")
                .append(subTokenType.name())
                .append(" {\n")
                .append("      baseType: ")
                .append(subTokenType.getBaseType().name())
                .append("\n")
                .append("      constraint: ");

            // Add the appropriate constraint based on the base type
            if (subTokenType.getIntConstraint() != null) {
                sb.append("Integer constraint");
            } else if (subTokenType.getStringConstraint() != null) {
                sb.append("String constraint");
            }

            sb.append("\n").append("      timestampComponentType: ").append(subTokenType.getTimestampComponentType()).append("\n");

            sb.append("      description: ").append(subTokenType.getDescription()).append("\n").append("    }\n");
        });
        sb.append("  }\n\n");

        // TokenTypes
        sb.append("  TokenTypes {\n");
        tokenTypes.forEach(tokenType -> {
            sb.append("    ")
                .append(tokenType.name())
                .append(" {\n")
                .append("      encodingType: ")
                .append(tokenType.encodingType())
                .append("\n")
                .append("      format: ")
                .append(tokenType.format())
                .append("\n")
                .append("      description: ")
                .append(tokenType.description())
                .append("\n")
                .append("    }\n");
        });
        sb.append("  }\n\n");

        // MultiTokenTypes
        sb.append("  MultiTokenTypes {\n");
        multiTokenTypes.forEach(multiTokenType -> {
            sb.append("    ")
                .append(multiTokenType.name())
                .append(" {\n")
                .append("      encodingType: ")
                .append(multiTokenType.encodingType())
                .append("\n")
                .append("      format: ")
                .append(multiTokenType.getFormat())
                .append("\n")
                .append("      description: ")
                .append(multiTokenType.getDescription())
                .append("\n")
                .append("    }\n");
        });
        sb.append("  }\n");

        sb.append("}");
        return sb.toString();
    }

    private void printCharArray(char[] tokenDelimiters, StringBuilder sb) {
        for (int i = 0; i < tokenDelimiters.length; i++) {
            char character = tokenDelimiters[i];
            sb.append(escapeChar(character));
            if (i < tokenDelimiters.length - 1) {
                sb.append(",");
            }
        }
    }

    private String escapeChar(char c) {
        return switch (c) {
            case '\t' -> "\\t";
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            case '\f' -> "\\f";
            case '\b' -> "\\b";
            case '\\' -> "\\\\";
            case '\"' -> "\\\"";
            case '\'' -> "\\'";
            case ' ' -> "SPACE";
            default -> {
                if (Character.isISOControl(c)) {
                    yield String.format(Locale.ROOT, "\\u%04x", (int) c);
                }
                yield String.valueOf(c);
            }
        };
    }

    private static String getConfigValue(Map<String, Object> typeMap, String symbol) {
        String ret = (String) typeMap.get(symbol);
        return ret == null ? null : ret.trim();
    }

    private static Map<String, Object> readYamlFileToMap() throws IOException {
        try (
            InputStream schemaFileIS = Schema.class.getResourceAsStream(SCHEMA_PATH);
            XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML)
                .createParser(XContentParserConfiguration.EMPTY, schemaFileIS)
        ) {
            return yamlParser.map();
        }
    }
}
