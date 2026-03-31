/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptionsTests extends ESTestCase {

    public void testNullOptions_SingleDataTypeAllowed() {
        Map<String, DataType> allowedOptions = Map.of("keyword_option", DataType.KEYWORD);
        Expression.TypeResolution resolution = Options.resolve(null, Source.EMPTY, TypeResolutions.ParamOrdinal.DEFAULT, allowedOptions);

        assertTrue(resolution.resolved());
    }

    public void testSingleEntryOptions_SingleDataTypeAllowed_ShouldResolve() {
        Map<String, DataType> allowedOptions = Map.of("keyword_option", DataType.KEYWORD);
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());
    }

    public void testSingleEntryOptions_SingleDataTypeAllowed_UnknownOption_ShouldNotResolve() {
        Map<String, DataType> allowedOptions = Map.of("keyword_option", DataType.KEYWORD);
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "unknown_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testMultipleEntryOptions_SingleDataTypeAllowed_ShouldResolve() {
        Map<String, DataType> allowedOptions = Map.of(
            "keyword_option",
            DataType.KEYWORD,
            "int_option",
            DataType.INTEGER,
            "double_option",
            DataType.DOUBLE
        );
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "keyword_option"),
                Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)),
                Literal.keyword(Source.EMPTY, "int_option"),
                Literal.integer(Source.EMPTY, 1),
                Literal.keyword(Source.EMPTY, "double_option"),
                Literal.fromDouble(Source.EMPTY, 1.0)
            )
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());
    }

    public void testMultipleEntryOptions_SingleDataTypeAllowed_UnknownOption_ShouldNotResolve() {
        Map<String, DataType> allowedOptions = Map.of(
            "keyword_option",
            DataType.KEYWORD,
            "int_option",
            DataType.INTEGER,
            "double_option",
            DataType.DOUBLE
        );
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "unknown_option"),
                Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)),
                Literal.keyword(Source.EMPTY, "int_option"),
                Literal.integer(Source.EMPTY, 1),
                Literal.keyword(Source.EMPTY, "double_option"),
                Literal.fromDouble(Source.EMPTY, 1.0)
            )
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testSingleEntryOptions_NullDataType_ShouldNotResolve() {
        Map<String, DataType> allowedOptions = new HashMap<>();
        allowedOptions.put("keyword_option", null);
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testSingleEntryOptions_SingleDataTypeAllowed_MapExpressionAsValue_ShouldNotResolve() {
        Map<String, DataType> allowedOptions = Map.of("map_option", DataType.OBJECT);
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "map_option"),
                new MapExpression(
                    Source.EMPTY,
                    List.of(Literal.keyword(Source.EMPTY, "some_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
                )
            )
        );
        Expression.TypeResolution resolution = Options.resolve(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testNullOptions_MultipleDataTypesAllowed() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_text_option", List.of(DataType.KEYWORD));
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            null,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());
    }

    public void testSingleEntryOptions_MultipleDataTypesAllowed_ShouldResolve() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_text_option", List.of(DataType.KEYWORD, DataType.TEXT));

        // Keyword resolution
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_text_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());

        // Text resolution
        mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_text_option"), Literal.text(Source.EMPTY, randomAlphaOfLength(10)))
        );
        resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());
    }

    public void testSingleEntryOptions_MultipleDataTypesAllowed_UnknownOption_ShouldNotResolve() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_string_option", List.of(DataType.KEYWORD, DataType.TEXT));
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "unknown_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testMultipleEntryOptions_MultipleDataTypesAllowed_ShouldResolve() {
        Map<String, Collection<DataType>> allowedOptions = Map.of(
            "keyword_text_option",
            List.of(DataType.KEYWORD, DataType.TEXT),
            "double_int_option",
            List.of(DataType.DOUBLE, DataType.INTEGER)
        );

        // Keyword & double resolution
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "keyword_text_option"),
                Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)),
                Literal.keyword(Source.EMPTY, "double_int_option"),
                Literal.integer(Source.EMPTY, randomInt())
            )
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());

        // Text & double resolution
        mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "keyword_text_option"),
                Literal.text(Source.EMPTY, randomAlphaOfLength(10)),
                Literal.keyword(Source.EMPTY, "double_int_option"),
                Literal.fromDouble(Source.EMPTY, randomDouble())
            )
        );
        resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.resolved());
    }

    public void testMultipleEntryOptions_MultipleDataTypesAllowed_UnknownOption_ShouldNotResolve() {
        Map<String, Collection<DataType>> allowedOptions = Map.of(
            "keyword_text_option",
            List.of(DataType.KEYWORD, DataType.TEXT),
            "double_int_option",
            List.of(DataType.DOUBLE, DataType.INTEGER)
        );
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "unknown_option"),
                Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)),
                Literal.keyword(Source.EMPTY, "double_int_option"),
                Literal.integer(Source.EMPTY, randomInt())
            )
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testSingleEntryOptions_MultipleDataTypesAllowed_NullDataType_ShouldNotResolve() {
        Collection<DataType> allowedDataTypes = new ArrayList<>();
        allowedDataTypes.add(null);

        Map<String, Collection<DataType>> allowedOptions = Map.of("null_option", allowedDataTypes);
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "null_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testSingleEntryOptions_MultipleDataTypeAllowed_MapExpressionAsValue_ShouldNotResolve() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("map_option", List.of(DataType.OBJECT, DataType.TEXT));
        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "map_option"),
                new MapExpression(
                    Source.EMPTY,
                    List.of(Literal.keyword(Source.EMPTY, "some_option"), Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)))
                )
            )
        );
        Expression.TypeResolution resolution = Options.resolveWithMultipleDataTypesAllowed(
            mapExpression,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertTrue(resolution.unresolved());
    }

    public void testPopulateMapWithExpressions_SingleEntry_KeywordDataType() throws InvalidArgumentException {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_option", List.of(DataType.KEYWORD));
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_option"), Literal.keyword(Source.EMPTY, "test_value"))
        );

        Options.populateMapWithExpressionsMultipleDataTypesAllowed(
            mapExpression,
            optionsMap,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertEquals(1, optionsMap.size());
        assertTrue(optionsMap.containsKey("keyword_option"));
        assertTrue(optionsMap.get("keyword_option") instanceof Literal);
        Literal storedLiteral = (Literal) optionsMap.get("keyword_option");
        assertEquals(DataType.KEYWORD, storedLiteral.dataType());
        assertEquals("test_value", ((BytesRef) storedLiteral.value()).utf8ToString());
    }

    public void testPopulateMapWithExpressions_SingleEntry_MultipleAllowedDataTypes_Keyword() throws InvalidArgumentException {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_text_option", List.of(DataType.KEYWORD, DataType.TEXT));
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_text_option"), Literal.keyword(Source.EMPTY, "keyword_value"))
        );

        Options.populateMapWithExpressionsMultipleDataTypesAllowed(
            mapExpression,
            optionsMap,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertEquals(1, optionsMap.size());
        assertTrue(optionsMap.containsKey("keyword_text_option"));
        Literal storedLiteral = (Literal) optionsMap.get("keyword_text_option");
        assertEquals(DataType.KEYWORD, storedLiteral.dataType());
        assertEquals("keyword_value", ((BytesRef) storedLiteral.value()).utf8ToString());
    }

    public void testPopulateMapWithExpressions_MultipleEntries() throws InvalidArgumentException {
        Map<String, Collection<DataType>> allowedOptions = Map.of(
            "keyword_text_option",
            List.of(DataType.KEYWORD, DataType.TEXT),
            "double_int_option",
            List.of(DataType.DOUBLE, DataType.INTEGER)
        );
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "keyword_text_option"),
                Literal.keyword(Source.EMPTY, "keyword_value"),
                Literal.keyword(Source.EMPTY, "double_int_option"),
                Literal.integer(Source.EMPTY, 42)
            )
        );

        Options.populateMapWithExpressionsMultipleDataTypesAllowed(
            mapExpression,
            optionsMap,
            Source.EMPTY,
            TypeResolutions.ParamOrdinal.DEFAULT,
            allowedOptions
        );

        assertEquals(2, optionsMap.size());

        // Check first option
        assertTrue(optionsMap.containsKey("keyword_text_option"));
        Literal firstLiteral = (Literal) optionsMap.get("keyword_text_option");
        assertEquals(DataType.KEYWORD, firstLiteral.dataType());
        assertEquals("keyword_value", ((BytesRef) firstLiteral.value()).utf8ToString());

        // Check second option
        assertTrue(optionsMap.containsKey("double_int_option"));
        Literal secondLiteral = (Literal) optionsMap.get("double_int_option");
        assertEquals(DataType.INTEGER, secondLiteral.dataType());
        assertEquals(42, secondLiteral.value());
    }

    public void testPopulateMapWithExpressions_UnknownOption_ShouldThrowException() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("known_option", List.of(DataType.KEYWORD));
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "unknown_option"), Literal.keyword(Source.EMPTY, "value"))
        );

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            Options.populateMapWithExpressionsMultipleDataTypesAllowed(
                mapExpression,
                optionsMap,
                Source.EMPTY,
                TypeResolutions.ParamOrdinal.DEFAULT,
                allowedOptions
            );
        });

        assertTrue(exception.getMessage().contains("Invalid option [unknown_option]"));
        assertTrue(exception.getMessage().contains("expected one of [known_option]"));
    }

    public void testPopulateMapWithExpressions_WrongDataType_ShouldThrowException() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("keyword_only_option", List.of(DataType.KEYWORD));
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "keyword_only_option"), Literal.text(Source.EMPTY, "text_value"))
        );

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            Options.populateMapWithExpressionsMultipleDataTypesAllowed(
                mapExpression,
                optionsMap,
                Source.EMPTY,
                TypeResolutions.ParamOrdinal.DEFAULT,
                allowedOptions
            );
        });

        assertTrue(exception.getMessage().contains("Invalid option [keyword_only_option]"));
        assertTrue(exception.getMessage().contains("allowed types"));
    }

    public void testPopulateMapWithExpressions_EmptyAllowedDataTypes_ShouldThrowException() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("empty_option", List.of());
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "empty_option"), Literal.keyword(Source.EMPTY, "value"))
        );

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            Options.populateMapWithExpressionsMultipleDataTypesAllowed(
                mapExpression,
                optionsMap,
                Source.EMPTY,
                TypeResolutions.ParamOrdinal.DEFAULT,
                allowedOptions
            );
        });

        assertTrue(exception.getMessage().contains("Invalid option [empty_option]"));
    }

    public void testPopulateMapWithExpressions_NullAllowedDataTypes_ShouldThrowException() {
        Map<String, Collection<DataType>> allowedOptions = new HashMap<>();
        allowedOptions.put("null_option", null);

        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression mapExpression = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "null_option"), Literal.keyword(Source.EMPTY, "value"))
        );

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            Options.populateMapWithExpressionsMultipleDataTypesAllowed(
                mapExpression,
                optionsMap,
                Source.EMPTY,
                TypeResolutions.ParamOrdinal.DEFAULT,
                allowedOptions
            );
        });

        assertTrue(exception.getMessage().contains("Invalid option [null_option]"));
    }

    public void testPopulateMapWithExpressions_NonLiteralValue_ShouldThrowException() {
        Map<String, Collection<DataType>> allowedOptions = Map.of("map_option", List.of(DataType.OBJECT));
        Map<String, Object> optionsMap = new HashMap<>();

        MapExpression nestedMap = new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "nested_key"), Literal.keyword(Source.EMPTY, "nested_value"))
        );

        MapExpression mapExpression = new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "map_option"), nestedMap));

        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            Options.populateMapWithExpressionsMultipleDataTypesAllowed(
                mapExpression,
                optionsMap,
                Source.EMPTY,
                TypeResolutions.ParamOrdinal.DEFAULT,
                allowedOptions
            );
        });

        assertTrue(exception.getMessage().contains("Invalid option [map_option]"));
    }
}
