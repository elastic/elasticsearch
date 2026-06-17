/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class SchemaCacheEntryTests extends ESTestCase {

    public void testFromSourceMetadataPreservesIdentityFields() {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        SourceMetadata meta = meta(schema, "parquet", "s3://bucket/file.parquet", Map.of("k", "v"), Map.of("c", "d"), null);

        SchemaCacheEntry entry = SchemaCacheEntry.from(meta);

        assertEquals("parquet", entry.sourceType());
        assertEquals("s3://bucket/file.parquet", entry.location());
        assertEquals(Map.of("c", "d"), entry.connectorConfig());
        List<Attribute> rebuilt = entry.toAttributes();
        assertEquals(2, rebuilt.size());
        assertEquals("id", rebuilt.get(0).name());
        assertEquals(DataType.INTEGER, rebuilt.get(0).dataType());
        assertEquals("name", rebuilt.get(1).name());
        assertEquals(DataType.KEYWORD, rebuilt.get(1).dataType());
    }

    public void testFromSourceMetadataEmbedsStatisticsIntoSafeMetadata() {
        SourceStatistics stats = new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(42_000L);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(99_000L);
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return Optional.empty();
            }
        };
        SourceMetadata meta = meta(List.of(attr("x", DataType.LONG)), "parquet", "p", Map.of("orig", true), Map.of(), stats);

        SchemaCacheEntry entry = SchemaCacheEntry.from(meta);

        // Stats live as flat keys inside safeMetadata — same shape every existing reader expects.
        assertEquals(42_000L, entry.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(99_000L, entry.safeMetadata().get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        // Original metadata entries survive alongside.
        assertEquals(true, entry.safeMetadata().get("orig"));
    }

    public void testFromSourceMetadataPassesSafeMetadataThroughWhenStatisticsAbsent() {
        SourceMetadata meta = meta(List.of(attr("x", DataType.LONG)), "parquet", "p", Map.of("k", 1), Map.of(), null);

        SchemaCacheEntry entry = SchemaCacheEntry.from(meta);

        assertEquals(Map.of("k", 1), entry.safeMetadata());
        assertNull(entry.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    public void testFromSourceMetadataEmbedsColumnStatistics() {
        SourceStatistics stats = new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(10);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.empty();
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return Optional.of(Map.of("id", new ColumnStatistics() {
                    @Override
                    public OptionalLong nullCount() {
                        return OptionalLong.of(3);
                    }

                    @Override
                    public OptionalLong distinctCount() {
                        return OptionalLong.empty();
                    }

                    @Override
                    public Optional<Object> minValue() {
                        return Optional.of(7L);
                    }

                    @Override
                    public Optional<Object> maxValue() {
                        return Optional.of(99L);
                    }

                    @Override
                    public OptionalLong sizeInBytes() {
                        return OptionalLong.of(40);
                    }
                }));
            }
        };
        SourceMetadata meta = meta(List.of(attr("id", DataType.LONG)), "parquet", "p", Map.of(), Map.of(), stats);

        SchemaCacheEntry entry = SchemaCacheEntry.from(meta);

        assertEquals(Long.valueOf(3), SourceStatisticsSerializer.extractColumnNullCount(entry.safeMetadata(), "id"));
        assertEquals(7L, SourceStatisticsSerializer.extractColumnMin(entry.safeMetadata(), "id"));
        assertEquals(99L, SourceStatisticsSerializer.extractColumnMax(entry.safeMetadata(), "id"));
        assertEquals(Long.valueOf(40), SourceStatisticsSerializer.extractColumnSizeBytes(entry.safeMetadata(), "id"));
    }

    public void testToAttributesReturnsFreshNameIdsOnEachCall() {
        SchemaCacheEntry entry = SchemaCacheEntry.from(meta(List.of(attr("a", DataType.INTEGER)), "p", "x", Map.of(), Map.of(), null));

        List<Attribute> first = entry.toAttributes();
        List<Attribute> second = entry.toAttributes();

        // Fresh ReferenceAttribute instances every call (avoids NameId sharing across queries —
        // the load-bearing reason SchemaCacheEntry stores raw arrays, not List<Attribute>).
        assertNotSame(first.get(0), second.get(0));
        NameId firstId = first.get(0).id();
        NameId secondId = second.get(0).id();
        assertNotEquals(firstId, secondId);
    }

    public void testToAttributesPreservesNullabilityAndSyntheticFlags() {
        SchemaCacheEntry entry = SchemaCacheEntry.from(
            List.of(
                new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG, Nullability.TRUE, null, false),
                new ReferenceAttribute(Source.EMPTY, null, "s", DataType.KEYWORD, Nullability.FALSE, null, true)
            ),
            "parquet",
            "p",
            Map.of(),
            Map.of()
        );

        List<Attribute> rebuilt = entry.toAttributes();
        assertEquals(Nullability.TRUE, rebuilt.get(0).nullable());
        assertFalse(rebuilt.get(0).synthetic());
        assertEquals(Nullability.FALSE, rebuilt.get(1).nullable());
        assertTrue(rebuilt.get(1).synthetic());
    }

    public void testFromPrimitivesRejectsMismatchedColumnArrays() {
        // Direct primitive factory: this validation lives in the record's compact constructor.
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new SchemaCacheEntry(
                new String[] { "a", "b" },
                new DataType[] { DataType.INTEGER },
                new Nullability[] { Nullability.TRUE },
                new boolean[] { false },
                "parquet",
                "p",
                Map.of(),
                Map.of(),
                0L
            )
        );
        assertTrue(ex.getMessage().contains("same length"));
    }

    public void testConstructorDefensivelyCopiesMaps() {
        // safeMetadata + connectorConfig must be defensive-copied so mutating the caller's map
        // after construction can't leak into cached state.
        Map<String, Object> mutableMeta = new HashMap<>();
        mutableMeta.put("k", "v");
        Map<String, Object> mutableConfig = new HashMap<>();
        mutableConfig.put("c", 1);

        SchemaCacheEntry entry = SchemaCacheEntry.from(List.of(attr("x", DataType.INTEGER)), "p", "l", mutableMeta, mutableConfig);

        mutableMeta.put("leaked", "v");
        mutableConfig.put("leaked", 1);

        assertEquals(Set.of("k"), entry.safeMetadata().keySet());
        assertEquals(Set.of("c"), entry.connectorConfig().keySet());
    }

    public void testConstructorTreatsNullMapsAsEmpty() {
        SchemaCacheEntry entry = SchemaCacheEntry.from(List.of(attr("x", DataType.INTEGER)), "p", "l", null, null);

        assertEquals(Map.of(), entry.safeMetadata());
        assertEquals(Map.of(), entry.connectorConfig());
    }

    public void testEstimatedBytesScalesWithColumnCount() {
        SchemaCacheEntry small = SchemaCacheEntry.from(List.of(attr("a", DataType.INTEGER)), "p", "l", Map.of(), Map.of());
        SchemaCacheEntry large = SchemaCacheEntry.from(
            List.of(attr("a", DataType.INTEGER), attr("b", DataType.LONG), attr("c", DataType.KEYWORD)),
            "p",
            "l",
            Map.of(),
            Map.of()
        );

        assertTrue("estimatedBytes must grow with column count", large.estimatedBytes() > small.estimatedBytes());
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, null, null, false);
    }

    private static SourceMetadata meta(
        List<Attribute> schema,
        String sourceType,
        String location,
        Map<String, Object> sourceMetadata,
        Map<String, Object> config,
        SourceStatistics stats
    ) {
        return new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return sourceType;
            }

            @Override
            public String location() {
                return location;
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return sourceMetadata;
            }

            @Override
            public Map<String, Object> config() {
                return config;
            }

            @Override
            public Optional<SourceStatistics> statistics() {
                return Optional.ofNullable(stats);
            }
        };
    }
}
