/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExternalRelationSerializationTests extends AbstractLogicalPlanSerializationTests<ExternalRelation> {

    public static ExternalRelation randomExternalRelation() {
        String sourcePath = "s3://bucket/" + randomAlphaOfLength(8) + ".parquet";
        String sourceType = randomFrom("parquet", "csv", "file", "iceberg");
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        Map<String, Object> config = randomBoolean() ? Map.of() : Map.of("endpoint", "https://s3.example.com");
        Map<String, Object> sourceMetadata = randomBoolean() ? Map.of() : Map.of("schema_version", 1);
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(randomSource(), sourcePath, metadata, output);
    }

    @Override
    protected ExternalRelation createTestInstance() {
        return randomExternalRelation();
    }

    @Override
    protected ExternalRelation mutateInstance(ExternalRelation instance) throws IOException {
        String sourcePath = instance.sourcePath();
        List<Attribute> output = instance.output();
        String sourceType = instance.metadata().sourceType();
        Map<String, Object> config = instance.metadata().config();
        Map<String, Object> sourceMetadata = instance.metadata().sourceMetadata();

        switch (between(0, 2)) {
            case 0 -> sourcePath = randomValueOtherThan(sourcePath, () -> "s3://bucket/" + randomAlphaOfLength(8) + ".parquet");
            case 1 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 2 -> sourceType = randomValueOtherThan(sourceType, () -> randomFrom("parquet", "csv", "file", "iceberg"));
            default -> throw new IllegalStateException();
        }
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(instance.source(), sourcePath, metadata, output);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
