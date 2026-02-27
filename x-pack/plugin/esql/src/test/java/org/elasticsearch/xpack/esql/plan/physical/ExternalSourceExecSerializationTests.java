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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExternalSourceExec> {

    public static ExternalSourceExec randomExternalSourceExec() {
        Source source = randomSource();
        String sourcePath = "s3://bucket/" + randomAlphaOfLength(8) + ".parquet";
        String sourceType = randomFrom("parquet", "csv", "file");
        List<Attribute> attributes = randomFieldAttributes(1, 5, false);
        Map<String, Object> config = randomBoolean() ? Map.of() : Map.of("endpoint", "https://s3.example.com");
        Map<String, Object> sourceMetadata = randomBoolean() ? Map.of() : Map.of("schema_version", 1);
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
            estimatedRowSize,
            null,
            splits
        );
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
        return new FileSplit(
            "file",
            path,
            randomLongBetween(0, 1000),
            randomLongBetween(1, 10000),
            ".parquet",
            Map.of(),
            randomBoolean() ? Map.of() : Map.of("year", randomIntBetween(2020, 2026))
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
            estimatedRowSize,
            null,
            splits
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
