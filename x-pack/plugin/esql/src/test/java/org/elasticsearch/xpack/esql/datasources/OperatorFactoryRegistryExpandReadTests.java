/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Only the file factory hands an expand read to its reader. A source factory that would drop it, a plugin's own
 * here, is refused by name before it is built, rather than returning parent rows a plan would bind as element rows.
 */
public class OperatorFactoryRegistryExpandReadTests extends ESTestCase {

    public void testNonFileFactoryRefusesAnExpandRead() {
        OperatorFactoryRegistry registry = new OperatorFactoryRegistry(Map.of(), Map.of("flight", context -> {
            throw new AssertionError("a factory that drops the expand read must not be built");
        }), Runnable::run, Runnable::run);
        assertRefused(registry, "flight");
    }

    /** A registered source factory that is not the file factory is refused too, before its own operator factory is asked. */
    public void testNonFileSourceFactoryRefusesAnExpandRead() {
        ExternalSourceFactory other = new ExternalSourceFactory() {
            @Override
            public String type() {
                return "table";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                throw new AssertionError("metadata is not resolved when the operator is built");
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public SourceOperatorFactoryProvider operatorFactory() {
                return context -> { throw new AssertionError("a factory that drops the expand read must not be built"); };
            }
        };
        OperatorFactoryRegistry registry = new OperatorFactoryRegistry(Map.of("table", other), Map.of(), Runnable::run, Runnable::run);
        assertRefused(registry, "table");
    }

    private static void assertRefused(OperatorFactoryRegistry registry, String sourceType) {
        SourceOperatorContext context = SourceOperatorContext.builder()
            .sourceType(sourceType)
            .path(StoragePath.of("s3://bucket/data.parquet"))
            .attributes(List.of())
            .executor(Runnable::run)
            .expandRead(new ExpandRead("items", List.of(new ExpandRead.Leaf("qty", DataType.LONG))), FormatReader.NO_LIMIT)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> registry.factory(context));
        assertThat(e.getMessage(), containsString("cannot be read through the [" + sourceType + "] source, which does not expand rows"));
    }
}
