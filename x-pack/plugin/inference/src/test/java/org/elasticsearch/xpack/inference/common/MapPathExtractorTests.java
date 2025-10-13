/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MapPathExtractorTests extends ESTestCase {
    public void testExtract_RetrievesListOfLists() {
        Map<String, Object> input = Map.of(
            "result",
            Map.of("embeddings", List.of(Map.of("index", 0, "embedding", List.of(1, 2)), Map.of("index", 1, "embedding", List.of(3, 4))))
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result.embeddings[*].embedding"),
            is(
                new MapPathExtractor.Result(
                    List.of(List.of(1, 2), List.of(3, 4)),
                    List.of("result.embeddings", "result.embeddings.embedding")
                )
            )
        );
    }

    public void testExtract_IteratesListOfMapsToListOfStrings() {
        Map<String, Object> input = Map.of(
            "result",
            List.of(Map.of("key", List.of("value1", "value2")), Map.of("key", List.of("value3", "value4")))
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result[*].key[*]"),
            is(
                new MapPathExtractor.Result(
                    List.of(List.of("value1", "value2"), List.of("value3", "value4")),
                    List.of("result", "result.key")
                )
            )
        );
    }

    public void testExtract_IteratesListOfMapsToListOfStrings_WithoutFinalArraySyntax() {
        Map<String, Object> input = Map.of(
            "result",
            List.of(Map.of("key", List.of("value1", "value2")), Map.of("key", List.of("value3", "value4")))
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result[*].key"),
            is(
                new MapPathExtractor.Result(
                    List.of(List.of("value1", "value2"), List.of("value3", "value4")),
                    List.of("result", "result.key")
                )
            )
        );
    }

    public void testExtract_IteratesListOfMapsToListOfMapsOfStringToDoubles() {
        Map<String, Object> input = Map.of(
            "result",
            List.of(
                Map.of("key", List.of(Map.of("a", 1.1d), Map.of("a", 2.2d))),
                Map.of("key", List.of(Map.of("a", 3.3d), Map.of("a", 4.4d)))
            )
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result[*].key[*].a"),
            is(
                new MapPathExtractor.Result(
                    List.of(List.of(1.1d, 2.2d), List.of(3.3d, 4.4d)),
                    List.of("result", "result.key", "result.key.a")
                )
            )
        );
    }

    public void testExtract_IteratesSparseEmbeddingStyleMap_ExtractsMaps() {
        Map<String, Object> input = Map.of(
            "result",
            Map.of(
                "sparse_embeddings",
                List.of(
                    Map.of(
                        "index",
                        0,
                        "embedding",
                        List.of(Map.of("tokenId", 6, "weight", 0.123d), Map.of("tokenId", 100, "weight", -123d))
                    ),
                    Map.of(
                        "index",
                        1,
                        "embedding",
                        List.of(Map.of("tokenId", 7, "weight", 0.456d), Map.of("tokenId", 200, "weight", -456d))
                    )
                )
            )
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result.sparse_embeddings[*].embedding[*]"),
            is(
                new MapPathExtractor.Result(
                    List.of(
                        List.of(Map.of("tokenId", 6, "weight", 0.123d), Map.of("tokenId", 100, "weight", -123d)),
                        List.of(Map.of("tokenId", 7, "weight", 0.456d), Map.of("tokenId", 200, "weight", -456d))
                    ),
                    List.of("result.sparse_embeddings", "result.sparse_embeddings.embedding")
                )
            )
        );
    }

    public void testExtract_IteratesSparseEmbeddingStyleMap_ExtractsFieldFromMap() {
        Map<String, Object> input = Map.of(
            "result",
            Map.of(
                "sparse_embeddings",
                List.of(
                    Map.of(
                        "index",
                        0,
                        "embedding",
                        List.of(Map.of("tokenId", 6, "weight", 0.123d), Map.of("tokenId", 100, "weight", -123d))
                    ),
                    Map.of(
                        "index",
                        1,
                        "embedding",
                        List.of(Map.of("tokenId", 7, "weight", 0.456d), Map.of("tokenId", 200, "weight", -456d))
                    )
                )
            )
        );

        assertThat(
            MapPathExtractor.extract(input, "$.result.sparse_embeddings[*].embedding[*].tokenId"),
            is(
                new MapPathExtractor.Result(
                    List.of(List.of(6, 100), List.of(7, 200)),
                    List.of("result.sparse_embeddings", "result.sparse_embeddings.embedding", "result.sparse_embeddings.embedding.tokenId")
                )
            )
        );
    }

    public void testExtract_ReturnsNullForEmptyList() {
        Map<String, Object> input = Map.of();

        assertNull(MapPathExtractor.extract(input, "$.awesome"));
    }

    public void testExtract_ReturnsNull_WhenTheInputMapIsNull() {
        assertNull(MapPathExtractor.extract(null, "$.result"));
    }

    public void testExtract_ReturnsNull_WhenPathIsNull() {
        assertNull(MapPathExtractor.extract(Map.of("key", "value"), null));
    }

    public void testExtract_ReturnsNull_WhenPathIsWhiteSpace() {
        assertNull(MapPathExtractor.extract(Map.of("key", "value"), "    "));
    }

    public void testExtract_ThrowsException_WhenPathDoesNotStartWithDollarSign() {
        var exception = expectThrows(IllegalArgumentException.class, () -> MapPathExtractor.extract(Map.of("key", "value"), ".key"));
        assertThat(exception.getMessage(), is("Path [.key] must start with a dollar sign ($)"));
    }

    public void testExtract_ThrowsException_WhenCannotFindField() {
        Map<String, Object> input = Map.of("result", "key");

        var exception = expectThrows(IllegalArgumentException.class, () -> MapPathExtractor.extract(input, "$.awesome"));
        assertThat(exception.getMessage(), is("Unable to find field [awesome] in map"));
    }

    public void testExtract_ThrowsAnException_WhenThePathIsInvalid() {
        Map<String, Object> input = Map.of("result", "key");

        var exception = expectThrows(IllegalArgumentException.class, () -> MapPathExtractor.extract(input, "$awesome"));
        assertThat(exception.getMessage(), is("Invalid path received [awesome], unable to extract a field name."));
    }

    public void testExtract_ThrowsException_WhenMissingArraySyntax() {
        Map<String, Object> input = Map.of(
            "result",
            Map.of("embeddings", List.of(Map.of("index", 0, "embedding", List.of(1, 2)), Map.of("index", 1, "embedding", List.of(3, 4))))
        );

        var exception = expectThrows(
            IllegalArgumentException.class,
            // embeddings is missing [*] to indicate that it is an array
            () -> MapPathExtractor.extract(input, "$.result.embeddings.embedding")
        );
        assertThat(
            exception.getMessage(),
            is(
                "Current path [.embedding] matched the dot field pattern but the current object "
                    + "is not a map, found invalid type [List12] instead."
            )
        );
    }

    public void testExtract_ThrowsException_WhenHasArraySyntaxButIsAMap() {
        Map<String, Object> input = Map.of(
            "result",
            Map.of("embeddings", List.of(Map.of("index", 0, "embedding", List.of(1, 2)), Map.of("index", 1, "embedding", List.of(3, 4))))
        );

        var exception = expectThrows(
            IllegalArgumentException.class,
            // result is not an array
            () -> MapPathExtractor.extract(input, "$.result[*].embeddings[*].embedding")
        );
        assertThat(
            exception.getMessage(),
            is(
                "Current path [[*].embeddings[*].embedding] matched the array field pattern but the current "
                    + "object is not a list, found invalid type [Map1] instead."
            )
        );
    }

    public void testExtract_ReturnsAnEmptyList_WhenItIsEmpty() {
        Map<String, Object> input = Map.of("result", List.of());

        assertThat(MapPathExtractor.extract(input, "$.result"), is(new MapPathExtractor.Result(List.of(), List.of("result"))));
    }

    public void testExtract_ReturnsAnEmptyList_WhenItIsEmpty_PathIncludesArray() {
        Map<String, Object> input = Map.of("result", List.of());

        assertThat(MapPathExtractor.extract(input, "$.result[*]"), is(new MapPathExtractor.Result(List.of(), List.of("result"))));
    }

    public void testDotFieldPattern() {
        {
            var matcher = MapPathExtractor.DOT_FIELD_PATTERN.matcher(".abc.123");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is("abc"));
            assertThat(matcher.group(2), is(".123"));
        }
        {
            var matcher = MapPathExtractor.DOT_FIELD_PATTERN.matcher(".abc[*].123");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is("abc"));
            assertThat(matcher.group(2), is("[*].123"));
        }
        {
            var matcher = MapPathExtractor.DOT_FIELD_PATTERN.matcher(".abc[.123");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is("abc"));
            assertThat(matcher.group(2), is("[.123"));
        }
        {
            var matcher = MapPathExtractor.DOT_FIELD_PATTERN.matcher(".abc");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is("abc"));
            assertThat(matcher.group(2), is(""));
        }
    }

    public void testArrayWildcardPattern() {
        {
            var matcher = MapPathExtractor.ARRAY_WILDCARD_PATTERN.matcher("[*].abc.123");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is(".abc.123"));
        }
        {
            var matcher = MapPathExtractor.ARRAY_WILDCARD_PATTERN.matcher("[*]");
            assertTrue(matcher.matches());
            assertThat(matcher.group(1), is(""));
        }
        {
            var matcher = MapPathExtractor.ARRAY_WILDCARD_PATTERN.matcher("[1].abc");
            assertFalse(matcher.matches());
        }
        {
            var matcher = MapPathExtractor.ARRAY_WILDCARD_PATTERN.matcher("[].abc");
            assertFalse(matcher.matches());
        }
    }
}
