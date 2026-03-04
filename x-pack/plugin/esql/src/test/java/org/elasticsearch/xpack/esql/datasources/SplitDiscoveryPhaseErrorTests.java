/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SplitDiscoveryPhaseErrorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testUncheckedIOExceptionWrappedWithContext() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        SplitProvider failingProvider = ctx -> { throw new UncheckedIOException(new IOException("connection reset by peer")); };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("s3://bucket/data/*.parquet"));
        assertThat(e.getMessage(), containsString("parquet"));
        assertThat(e.getCause(), instanceOf(UncheckedIOException.class));
        assertThat(e.getCause().getCause().getMessage(), containsString("connection reset by peer"));
    }

    public void testRuntimeExceptionWrappedWithContext() {
        ExternalSourceExec exec = createExternalSourceExec("gcs://bucket/files/*.csv", "csv");
        SplitProvider failingProvider = ctx -> { throw new RuntimeException("unexpected error"); };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("csv", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("gcs://bucket/files/*.csv"));
        assertThat(e.getMessage(), containsString("csv"));
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
    }

    public void testElasticsearchExceptionNotDoubleWrapped() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        ElasticsearchException original = new ElasticsearchException("already wrapped");
        SplitProvider failingProvider = ctx -> { throw original; };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertSame(original, e);
    }

    public void testPermissionErrorIncludesSourcePath() {
        ExternalSourceExec exec = createExternalSourceExec("s3://secure-bucket/private/*.parquet", "parquet");
        SplitProvider failingProvider = ctx -> { throw new SecurityException("Access Denied (403)"); };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("s3://secure-bucket/private/*.parquet"));
        assertThat(e.getCause(), instanceOf(SecurityException.class));
    }

    public void testSuccessfulDiscoveryUnaffected() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        SplitProvider okProvider = ctx -> List.of();

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(okProvider)));

        assertTrue(result instanceof ExternalSourceExec);
        assertTrue(((ExternalSourceExec) result).splits().isEmpty());
    }

    // -- helpers --

    private static ExternalSourceExec createExternalSourceExec(String sourcePath, String sourceType) {
        List<Attribute> attrs = List.of(fieldAttr("id", DataType.LONG));
        return new ExternalSourceExec(SRC, sourcePath, sourceType, attrs, Map.of(), Map.of(), null, null, FileSet.UNRESOLVED);
    }

    private static Attribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalSourceFactory testFactory(SplitProvider provider) {
        return new ExternalSourceFactory() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public SplitProvider splitProvider() {
                return provider;
            }
        };
    }
}
