/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Notices raised while <em>resolving</em> an external source (as opposed to while reading it) must reach the client.
 * Resolution runs on the resolver's executor chain, where a response header written directly is lost, and its result is
 * cached, so a notice emitted only where it was raised would also vanish on the second run of the same query. Each case
 * here asserts through HTTP, the channel a client actually reads.
 */
// One node, so the second request in each cold-then-cached case reaches the same node and therefore the same caches; a
// randomised coordinating-only node could route it elsewhere and let that assertion pass cold.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalSourceResolutionWarningsIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    /** The same query must warn on its second run, when the schema comes from the cache, exactly as on its first. */
    public void testNullMarkerHintWarnsOnColdAndCachedResolve() throws Exception {
        Path dir = createTempDir().resolve("null_marker_hint");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,note\n1,\\N\n2,plain\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("null_marker_hint", StoragePath.fileUri(dir.resolve("a.csv")), Map.of("mode", "plain"));
        String query = "FROM " + dataset + " | SORT id | KEEP note";

        String hint = "null marker, but the current mode keeps it as literal text";
        assertThat("cold resolve", warningsOf(query), hasItem(containsString(hint)));
        assertThat("cached resolve", warningsOf(query), hasItem(containsString(hint)));
    }

    public void testEscapedModeQuoteOverrideWarningReachesClient() throws Exception {
        Path dir = createTempDir().resolve("escaped_quote");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,note\n1,x\n", StandardCharsets.UTF_8);
        String dataset = registerDataset(
            "escaped_quote",
            StoragePath.fileUri(dir.resolve("a.csv")),
            Map.of("mode", "escaped", "quote", "\"")
        );

        assertThat(warningsOf("FROM " + dataset + " | KEEP note"), hasItem(containsString("disables the escaped-mode decode")));
    }

    public void testFileExclusionWarningReachesClient() throws Exception {
        Path dir = createTempDir().resolve("exclusion");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,note\n1,x\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("_SUCCESS"), "", StandardCharsets.UTF_8);
        // The bare glob has no extension to infer the format from, so name it; a Spark _SUCCESS marker never has one either.
        String dataset = registerDataset("exclusion", StoragePath.fileUri(dir) + "/*", Map.of("format", "csv"));

        assertThat(
            warningsOf("FROM " + dataset + " | KEEP note"),
            hasItem(containsString("was excluded by the [file_exclusions] dataset setting"))
        );
    }

    /**
     * {@code first_file_wins} resolves through the listing cache, so the second run must replay the notice that the
     * cached expansion raised, exactly as the first did.
     */
    public void testFileExclusionWarnsOnColdAndCachedListing() throws Exception {
        Path dir = createTempDir().resolve("exclusion_cached");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,note\n1,x\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("_SUCCESS"), "", StandardCharsets.UTF_8);
        String dataset = registerDataset(
            "exclusion_cached",
            StoragePath.fileUri(dir) + "/*",
            Map.of("format", "csv", "schema_resolution", "first_file_wins")
        );
        String query = "FROM " + dataset + " | KEEP note";

        String notice = "was excluded by the [file_exclusions] dataset setting";
        assertThat("cold listing", warningsOf(query), hasItem(containsString(notice)));
        assertThat("cached listing", warningsOf(query), hasItem(containsString(notice)));
    }

    public void testReservedPartitionNameRenameWarningReachesClient() throws Exception {
        Path dir = createTempDir().resolve("reserved_partition");
        Path part = dir.resolve("_index=alpha");
        Files.createDirectories(part);
        Files.writeString(part.resolve("a.csv"), "id,note\n1,x\n", StandardCharsets.UTF_8);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(dir) + "/**/*.csv";
        String dataset = registerDataset("reserved_partition", glob, Map.of("hive_partitioning", true));

        assertThat(
            warningsOf("FROM " + dataset + " | KEEP note"),
            hasItem(containsString("partition column [_index] surfaced as [_partition._index]"))
        );
    }

    public void testKeywordWideningWarningReachesClient() throws Exception {
        Path dir = createTempDir().resolve("keyword_widening");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,col\n1,100\n2,200\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,col\n3,abc\n4,def\n", StandardCharsets.UTF_8);
        String dataset = registerDataset(
            "keyword_widening",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "union_by_name", "error_mode", "null_field")
        );

        assertThat(warningsOf("FROM " + dataset + " | SORT id | KEEP col"), hasItem(containsString("widened columns to keyword")));
    }

    /** Runs {@code query} over HTTP and returns the {@code Warning} header messages of the response. */
    private List<String> warningsOf(String query) throws Exception {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query).endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return response.getWarnings();
    }
}
