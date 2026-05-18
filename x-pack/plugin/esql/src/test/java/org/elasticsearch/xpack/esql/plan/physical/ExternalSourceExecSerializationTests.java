/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExternalSourceExec> {

    public static ExternalSourceExec randomExternalSourceExec() {
        Source source = randomSource();
        String sourcePath = "s3://bucket/" + randomAlphaOfLength(8) + ".parquet";
        String sourceType = randomFrom("parquet", "csv", "file");
        List<Attribute> attributes = randomFieldAttributes(1, 5, false);
        Map<String, Object> config = randomBoolean() ? Map.of() : Map.of("endpoint", "https://s3.example.com");
        Map<String, Object> sourceMetadata = randomBoolean() ? Map.of() : randomSourceMetadataWithStats();
        Integer estimatedRowSize = randomEstimatedRowSize();
        List<ExternalSplit> splits = randomSplits();
        return new ExternalSourceExec(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            splits
        );
    }

    private static Map<String, Object> randomSourceMetadataWithStats() {
        Map<String, Object> map = new HashMap<>();
        map.put("schema_version", 1);
        if (randomBoolean()) {
            map.put("_stats.row_count", randomLongBetween(0, 100_000));
            map.put("_stats.size_bytes", randomLongBetween(1000, 10_000_000));
            String intCol = randomAlphaOfLength(5);
            map.put("_stats.columns." + intCol + ".null_count", randomLongBetween(0, 1000));
            map.put("_stats.columns." + intCol + ".min", randomIntBetween(0, 100));
            map.put("_stats.columns." + intCol + ".max", randomIntBetween(100, 1000));
            String strCol = randomAlphaOfLength(5);
            map.put("_stats.columns." + strCol + ".null_count", randomLongBetween(0, 100));
            map.put("_stats.columns." + strCol + ".min", randomAlphaOfLength(5));
            map.put("_stats.columns." + strCol + ".max", randomAlphaOfLength(5));
        }
        return map;
    }

    static List<ExternalSplit> randomSplits() {
        int count = between(0, 4);
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(randomFileSplit());
        }
        return splits;
    }

    static FileSplit randomFileSplit() {
        StoragePath path = StoragePath.of("s3://bucket/data/" + randomAlphaOfLength(6) + ".parquet");
        Map<String, Object> statistics;
        if (randomBoolean()) {
            statistics = null;
        } else {
            Map<String, Object> stats = new HashMap<>();
            stats.put("_stats.row_count", randomLongBetween(100, 10000));
            stats.put("_stats.size_bytes", randomLongBetween(1000, 100000));
            if (randomBoolean()) {
                String colName = randomAlphaOfLength(4);
                stats.put("_stats.columns." + colName + ".null_count", randomLongBetween(0, 100));
                stats.put("_stats.columns." + colName + ".min", randomIntBetween(0, 50));
                stats.put("_stats.columns." + colName + ".max", randomIntBetween(50, 200));
            }
            if (randomBoolean()) {
                String strCol = randomAlphaOfLength(4);
                stats.put("_stats.columns." + strCol + ".null_count", randomLongBetween(0, 50));
                stats.put("_stats.columns." + strCol + ".min", randomAlphaOfLength(4));
                stats.put("_stats.columns." + strCol + ".max", randomAlphaOfLength(4));
            }
            statistics = Map.copyOf(stats);
        }
        return new FileSplit(
            "file",
            path,
            randomLongBetween(0, 1000),
            randomLongBetween(1, 10000),
            ".parquet",
            Map.of(),
            randomBoolean() ? Map.of() : Map.of("year", randomIntBetween(2020, 2026)),
            null,
            statistics
        );
    }

    @Override
    protected ExternalSourceExec createTestInstance() {
        return randomExternalSourceExec();
    }

    @Override
    protected ExternalSourceExec mutateInstance(ExternalSourceExec instance) throws IOException {
        String sourcePath = instance.sourcePath();
        String sourceType = instance.sourceType();
        List<Attribute> attributes = instance.output();
        Map<String, Object> config = instance.config();
        Map<String, Object> sourceMetadata = instance.sourceMetadata();
        Integer estimatedRowSize = instance.estimatedRowSize();
        List<ExternalSplit> splits = instance.splits();

        switch (between(0, 5)) {
            case 0 -> sourcePath = randomValueOtherThan(sourcePath, () -> "s3://bucket/" + randomAlphaOfLength(8) + ".parquet");
            case 1 -> sourceType = randomValueOtherThan(sourceType, () -> randomFrom("parquet", "csv", "file", "iceberg"));
            case 2 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(1, 5, false));
            case 3 -> config = randomValueOtherThan(config, () -> Map.of("key", randomAlphaOfLength(5)));
            case 4 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            case 5 -> splits = randomValueOtherThan(splits, ExternalSourceExecSerializationTests::randomSplits);
            default -> throw new IllegalStateException();
        }
        return new ExternalSourceExec(
            instance.source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            splits
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
