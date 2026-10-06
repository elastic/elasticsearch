/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class SplitDiscoveryPhaseErrorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testUncheckedIOExceptionWrappedWithContext() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        SplitProvider failingProvider = ctx -> { throw new UncheckedIOException(new IOException("connection reset by peer")); };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("[*.parquet]"));
        assertThat(e.getMessage(), containsString("of type [parquet]"));
        assertThat(e.getMessage(), containsString("connection reset by peer"));
        assertThat(e.getMessage(), not(containsString("bucket")));
        assertNull(e.getCause());
    }

    public void testRuntimeExceptionWrappedWithContext() {
        ExternalSourceExec exec = createExternalSourceExec("gcs://bucket/files/*.csv", "csv");
        SplitProvider failingProvider = ctx -> { throw new RuntimeException("unexpected error"); };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("csv", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("[*.csv]"));
        assertThat(e.getMessage(), containsString("of type [csv]"));
        assertThat(e.getMessage(), containsString("unexpected error"));
        assertThat(e.getMessage(), not(containsString("bucket")));
        assertNull(e.getCause());
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

        assertThat(e.getMessage(), containsString("[*.parquet]"));
        assertThat(e.getMessage(), containsString("Access Denied (403)"));
        assertThat(e.getMessage(), not(containsString("secure-bucket")));
        assertThat(e.getMessage(), not(containsString("private")));
        assertNull(e.getCause());
    }

    public void testSuccessfulDiscoveryUnaffected() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        SplitProvider okProvider = ctx -> SplitDiscoveryResult.EMPTY;

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(okProvider)));

        assertTrue(result instanceof ExternalSourceExec);
        assertTrue(((ExternalSourceExec) result).splits().isEmpty());
    }

    /**
     * A storage client's message relays what the remote said: an IAM denial names the principal and the resource. It
     * names no URI or host, so only its origin can keep it out of the response.
     */
    public void testStorageClientMessageIsWithheldFromAnIllegalArgument() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.csv", "csv");
        IllegalArgumentException original = new IllegalArgumentException(
            "User: arn:aws:sts::123456789012:assumed-role/reader/session is not authorized to perform: s3:ListBucket"
        );
        original.setStackTrace(
            new StackTraceElement[] { new StackTraceElement("software.amazon.awssdk.core.exception.SdkClientException", "create", null, 1) }
        );
        SplitProvider failingProvider = ctx -> { throw original; };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("csv", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString("[*.csv]"));
        assertThat(e.getMessage(), not(containsString("arn:aws")));
        assertNull(e.getCause());
    }

    /** A format library's message describes the file, so it still reaches the user. */
    public void testFormatLibraryMessageIsForwarded() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        String magic = "file is not a Parquet file. Expected magic number at tail, but found [1, 2, 3, 4]";
        IllegalArgumentException original = new IllegalArgumentException(magic);
        original.setStackTrace(
            new StackTraceElement[] { new StackTraceElement("org.apache.parquet.hadoop.ParquetFileReader", "readFooter", null, 1) }
        );
        SplitProvider failingProvider = ctx -> { throw original; };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertThat(e.getMessage(), containsString(magic));
    }

    /** An {@link ElasticsearchException} that is not an external one is rendered with its cause too, so it is detached. */
    public void testForeignElasticsearchExceptionIsDetachedFromItsCause() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        ElasticsearchStatusException original = new ElasticsearchStatusException(
            "listing refused",
            RestStatus.FORBIDDEN,
            new IOException("GET s3://bucket/data/ failed")
        );
        SplitProvider failingProvider = ctx -> { throw original; };

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertEquals(RestStatus.FORBIDDEN, e.status());
        assertEquals("listing refused", e.getMessage());
        assertNull(e.getCause());
    }

    /** The result carries no cause, so query failure ranking would not find a cancellation left wrapped. */
    public void testWrappedCancellationIsReportedAsTheCancellation() {
        ExternalSourceExec exec = createExternalSourceExec("s3://bucket/data/*.parquet", "parquet");
        SplitProvider failingProvider = ctx -> { throw new CompletionException(new TaskCancelledException("cancelled")); };

        TaskCancelledException e = expectThrows(
            TaskCancelledException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of("parquet", testFactory(failingProvider)))
        );

        assertNull(e.getCause());
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
