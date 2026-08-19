/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
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

    /**
     * A user-caused failure keeps its 400. Everything that is not an {@link ElasticsearchException} used to be
     * wrapped in a bare one, which maps to 500 -- so converting a config parser to {@link IllegalArgumentException}
     * bought nothing on the only query path that reaches it: the status was thrown away one frame up. The wrap now
     * preserves the type while still adding the source path and format, so the parser conversions are not cosmetic.
     */
    public void testIllegalArgumentExceptionKeepsClientStatusAndGainsContext() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.csv", "csv");
        IllegalArgumentException original = new IllegalArgumentException("Invalid value for [target_split_size]: [0b]; must be positive");
        SplitProvider failingProvider = ctx -> { throw original; };

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("csv", testFactory(failingProvider)))
        );

        assertEquals("a user-caused split-discovery failure is a client error", RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        assertThat(e.getMessage(), containsString("s3://bucket/data/*.csv"));
        assertThat(e.getMessage(), containsString("csv"));
        assertSame("the original failure must be preserved as the cause", original, e.getCause());
    }

    /**
     * The escape, not the throw. {@link #testIllegalArgumentExceptionKeepsClientStatusAndGainsContext} hands the
     * phase a hand-built {@link IllegalArgumentException}, so it pins the wrap and nothing more: it stays green no
     * matter what {@code FileSplitProvider} actually throws. This runs the real provider over a real one-file list
     * with a real bad {@code target_split_size}, so it fails the moment the parser goes back to throwing a
     * {@code QlIllegalArgumentException} -- which the {@code catch (ElasticsearchException)} arm above rethrows
     * untouched, losing both the 400 and the source-path context this asserts.
     */
    public void testRealSplitProviderRejectsTargetSplitSizeAsClientErrorThroughThePhase() {
        StorageEntry file = new StorageEntry(StoragePath.of("s3://bucket/data/events.ndjson"), 3000, Instant.EPOCH);
        ExternalSourceExec exec = new ExternalSourceExec(
            SRC,
            "s3://bucket/data/*.ndjson",
            "ndjson",
            List.of(fieldAttr("id", DataType.LONG)),
            Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "0b"),
            Map.of(),
            null,
            null
        ).withFileList(GlobExpander.fileListOf(List.of(file), "s3://bucket/data/*.ndjson"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("ndjson", testFactory(new FileSplitProvider())))
        );

        assertEquals("an invalid target_split_size is the user's mistake, not ours", RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        assertThat(e.getMessage(), containsString("s3://bucket/data/*.ndjson"));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE));
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
        SplitProvider okProvider = ctx -> SplitDiscoveryResult.EMPTY;

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(okProvider)));

        assertTrue(result instanceof ExternalSourceExec);
        assertTrue(((ExternalSourceExec) result).splits().isEmpty());
    }

    // -- helpers --

    private static ExternalSourceExec createExternalSourceExec(String sourcePath, String sourceType) {
        List<Attribute> attrs = List.of(fieldAttr("id", DataType.LONG));
        return new ExternalSourceExec(SRC, sourcePath, sourceType, attrs, Map.of(), Map.of(), null, null).withFileList(FileList.UNRESOLVED);
    }

    private static Attribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalSourceFactory testFactory(SplitProvider provider) {
        return new ExternalSourceFactory() {

            @Override
            public void validateConfig(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException("test stub does not implement validation");
            }

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
