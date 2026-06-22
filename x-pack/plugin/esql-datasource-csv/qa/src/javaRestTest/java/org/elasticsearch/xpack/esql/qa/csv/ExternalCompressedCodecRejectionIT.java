/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.ClassRule;

import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;

/**
 * End-to-end negative test for the GA text-format codec gate (elastic/esql-planning#938): on release builds an
 * {@code EXTERNAL "...csv.bz2"} query must be rejected at planning time with the
 * {@code "compression codec [bzip2] is not supported; supported: uncompressed, gzip, zstd"} message produced by
 * {@code FormatReaderRegistry.byExtension}. bzip2 stands in for the four codecs the gate removes
 * (bzip2/snappy/lz4/brotli); it is the only one with cluster-side plugin + fixture coverage in this module.
 *
 * <p>The rejection fires during schema resolution ({@code FileSourceFactory.resolveMetadata} resolves the format
 * reader before opening the object), so the query fails fast without a reachable file or credentials — the
 * unreachable {@code http://} URL is never contacted.
 *
 * <p><b>When this runs.</b> The assertion is guarded by two independent gates that, today, never hold at the same
 * time, so the test is currently inert:
 * <ul>
 *   <li>{@link #testBzip2RejectedOnReleaseBuild()} skips unless the build is a release build
 *       ({@code Build.current().isSnapshot() == false}) — on snapshot the gate is bypassed and bzip2 is allowed; and</li>
 *   <li>it skips unless the cluster advertises the {@code external_command} capability — EXTERNAL is behind the
 *       {@code esql_external_datasources} feature flag (snapshot-only today).</li>
 * </ul>
 * Additionally this module's {@code javaRestTest} task is itself snapshot-only ({@code enabled = buildParams.snapshotBuild}
 * in build.gradle). The live release coverage of the same {@code FormatReaderRegistry.byExtension} code path is the unit
 * test {@code DataSourceModuleTests.testTextCodecsRejectedOnReleaseBuilds}. This IT becomes active automatically once
 * EXTERNAL graduates from the feature flag and the module is allowed to run on release builds, asserting the rejection
 * through the full REST + planning path.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalCompressedCodecRejectionIT extends ESRestTestCase {

    // The S3 endpoint is unused: this suite never reads a blob (the codec gate fires before any storage access),
    // so a placeholder address keeps the cluster wiring identical to the other suites without standing up a fixture.
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> "http://localhost:9");

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testBzip2RejectedOnReleaseBuild() throws Exception {
        assumeTrue(
            "EXTERNAL command must be available (esql_external_datasources feature flag)",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.EXTERNAL_COMMAND.capabilityName()))
        );
        assumeFalse("snapshot builds allow the full codec set; the gate is release-only", Build.current().isSnapshot());

        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", "EXTERNAL \"http://localhost:9/employees.csv.bz2\"").endObject();
            req.setJsonEntity(Strings.toString(b));
        }

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(req));
        assertThat(e.getMessage(), containsString("compression codec [bzip2] is not supported; supported: uncompressed, gzip, zstd"));
    }
}
