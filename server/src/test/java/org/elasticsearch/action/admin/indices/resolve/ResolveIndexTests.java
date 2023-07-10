/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Request;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.TransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.SystemIndices.EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.indices.SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.core.IsNull.notNullValue;

public class ResolveIndexTests extends ESTestCase {

    private final Object[][] indices = new Object[][] {
        // name, isClosed, isHidden, isSystem, isFrozen, dataStream, aliases
        { "logs-pgsql-prod-20200101", false, false, false, true, null, new String[] { "logs-pgsql-prod" } },
        { "logs-pgsql-prod-20200102", false, false, false, true, null, new String[] { "logs-pgsql-prod", "one-off-alias" } },
        { "logs-pgsql-prod-20200103", false, false, false, false, null, new String[] { "logs-pgsql-prod" } },
        { "logs-pgsql-test-20200101", true, false, false, false, null, new String[] { "logs-pgsql-test" } },
        { "logs-pgsql-test-20200102", false, false, false, false, null, new String[] { "logs-pgsql-test" } },
        { "logs-pgsql-test-20200103", false, false, false, false, null, new String[] { "logs-pgsql-test" } },
        { ".test-system-index", false, false, true, false, null, new String[] {} } };

    private final Object[][] dataStreams = new Object[][] {
        // name, numBackingIndices
        { "logs-mysql-prod", 4 },
        { "logs-mysql-test", 2 } };

    private ClusterState clusterState;
    private ThreadContext threadContext;

    private IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

    private long epochMillis;
    private String dateString;

    @Before
    public void setup() {
        epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        threadContext = createThreadContext();
        resolver = new IndexNameExpressionResolver(threadContext, EmptySystemIndices.INSTANCE);
        dateString = DataStream.DATE_FORMATTER.formatMillis(epochMillis);
        clusterState = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(dataStreams, indices)).build();
    }

    public void testResolveStarWithDefaultOptions() {
        String[] names = new String[] { "*" };
        IndicesOptions indicesOptions = Request.DEFAULT_INDICES_OPTIONS;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams);

        validateIndices(
            indices,
            ".test-system-index",
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test", "one-off-alias");

        validateDataStreams(dataStreams, "logs-mysql-prod", "logs-mysql-test");
    }

    public void testResolveStarWithAllOptions() {
        String[] names = randomFrom(new String[] { "*" }, new String[] { "_all" });
        IndicesOptions indicesOptions = randomFrom(
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN,
            IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN
        );
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams);
        validateIndices(
            indices,
            ".ds-logs-mysql-prod-" + dateString + "-000001",
            ".ds-logs-mysql-prod-" + dateString + "-000002",
            ".ds-logs-mysql-prod-" + dateString + "-000003",
            ".ds-logs-mysql-prod-" + dateString + "-000004",
            ".ds-logs-mysql-test-" + dateString + "-000001",
            ".ds-logs-mysql-test-" + dateString + "-000002",
            ".test-system-index",
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200101",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test", "one-off-alias");

        validateDataStreams(dataStreams, "logs-mysql-prod", "logs-mysql-test");
    }

    public void testResolveWithPattern() {
        String[] names = new String[] { "logs-pgsql*" };
        IndicesOptions indicesOptions = Request.DEFAULT_INDICES_OPTIONS;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams);

        validateIndices(
            indices,
            "logs-pgsql-prod-20200101",
            "logs-pgsql-prod-20200102",
            "logs-pgsql-prod-20200103",
            "logs-pgsql-test-20200102",
            "logs-pgsql-test-20200103"
        );

        validateAliases(aliases, "logs-pgsql-prod", "logs-pgsql-test");

        validateDataStreams(dataStreams, Strings.EMPTY_ARRAY);
    }

    public void testResolveWithMultipleNames() {
        String[] names = new String[] {
            ".ds-logs-mysql-prod-" + dateString + "-000003",
            "logs-pgsql-test-20200102",
            "one-off-alias",
            "logs-mysql-test" };
        IndicesOptions indicesOptions = IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();

        TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams);
        validateIndices(indices, ".ds-logs-mysql-prod-" + dateString + "-000003", "logs-pgsql-test-20200102");
        validateAliases(aliases, "one-off-alias");
        validateDataStreams(dataStreams, "logs-mysql-test");
    }

    public void testResolvePreservesBackingIndexOrdering() {
        Metadata.Builder builder = Metadata.builder();
        String dataStreamName = "my-data-stream";
        String[] names = { "not-in-order-2", "not-in-order-1", DataStream.getDefaultBackingIndexName(dataStreamName, 3, epochMillis) };
        List<IndexMetadata> backingIndices = Arrays.stream(names).map(n -> createIndexMetadata(n, true)).toList();
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }

        DataStream ds = DataStreamTestHelper.newInstance(dataStreamName, backingIndices.stream().map(IndexMetadata::getIndex).toList());
        builder.put(ds);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(builder.build()).build();

        IndicesOptions indicesOptions = IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();
        TransportAction.resolveIndices(new String[] { "*" }, indicesOptions, clusterState, resolver, indices, aliases, dataStreams);

        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getBackingIndices(), arrayContaining(names));
    }

    public void testResolveHiddenProperlyWithDateMath() {
        // set up with today's index and following day's index to avoid test failures due to execution time
        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu.MM.dd");
        Instant now = Instant.now(Clock.systemUTC());
        String todaySuffix = dateFormatter.format(now);
        String tomorrowSuffix = dateFormatter.format(now.plus(Duration.ofDays(1L)));
        Object[][] indices = new Object[][] {
            // name, isClosed, isHidden, isFrozen, dataStream, aliases
            { "logs-pgsql-prod-" + todaySuffix, false, true, false, false, null, Strings.EMPTY_ARRAY },
            { "logs-pgsql-prod-" + tomorrowSuffix, false, true, false, false, null, Strings.EMPTY_ARRAY } };
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(buildMetadata(new Object[][] {}, indices))
            .build();
        String[] requestedIndex = new String[] { "<logs-pgsql-prod-{now/d}>" };
        Set<String> resolvedIndices = resolver.resolveExpressions(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN, true, requestedIndex);
        assertThat(resolvedIndices.size(), is(1));
        assertThat(resolvedIndices, contains(oneOf("logs-pgsql-prod-" + todaySuffix, "logs-pgsql-prod-" + tomorrowSuffix)));
    }

    public void testSystemIndexAccess() {
        Metadata.Builder mdBuilder = buildMetadata(dataStreams, indices);
        SystemIndices systemIndices = addSystemIndex(mdBuilder);
        clusterState = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            threadContext = createThreadContext();
            if (randomBoolean()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "false");
            }
            threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "whatever");
            resolver = new IndexNameExpressionResolver(threadContext, systemIndices);
            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            TransportAction.resolveIndices(
                new String[] { ".*" },
                IndicesOptions.LENIENT_EXPAND_OPEN,
                clusterState,
                resolver,
                indices,
                aliases,
                dataStreams
            );
            // non net-new system indices are allowed even when no system indices are allowed
            assertThat(indices.stream().map(ResolvedIndex::getName).collect(Collectors.toList()), hasItem(is(".test-system-1")));
        }
        {
            threadContext = createThreadContext();
            threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "test-net-new-system");
            resolver = new IndexNameExpressionResolver(threadContext, systemIndices);
            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            TransportAction.resolveIndices(
                new String[] { ".*" },
                IndicesOptions.STRICT_EXPAND_OPEN,
                clusterState,
                resolver,
                indices,
                aliases,
                dataStreams
            );
            assertThat(indices.stream().map(ResolvedIndex::getName).collect(Collectors.toList()), hasItem(is(".test-system-1")));
            assertThat(indices.stream().map(ResolvedIndex::getName).collect(Collectors.toList()), hasItem(is(".test-net-new-system-1")));
            indices = new ArrayList<>();
            TransportAction.resolveIndices(
                new String[] { ".test-net*" },
                IndicesOptions.STRICT_EXPAND_OPEN,
                clusterState,
                resolver,
                indices,
                aliases,
                dataStreams
            );
            assertThat(indices.stream().map(ResolvedIndex::getName).collect(Collectors.toList()), not(hasItem(is(".test-system-1"))));
            assertThat(indices.stream().map(ResolvedIndex::getName).collect(Collectors.toList()), hasItem(is(".test-net-new-system-1")));
        }
    }

    public void testIgnoreUnavailableFalse() {
        String[] names = new String[] { "missing", "logs*" };
        IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams)
        );
        assertThat(infe.getMessage(), containsString("no such index [missing]"));
    }

    public void testAllowNoIndicesFalse() {
        String[] names = new String[] { "missing", "missing*" };
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, false, true, true);
        List<ResolvedIndex> indices = new ArrayList<>();
        List<ResolvedAlias> aliases = new ArrayList<>();
        List<ResolvedDataStream> dataStreams = new ArrayList<>();
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> TransportAction.resolveIndices(names, indicesOptions, clusterState, resolver, indices, aliases, dataStreams)
        );
        assertThat(infe.getMessage(), containsString("no such index [missing*]"));
    }

    private void validateIndices(List<ResolvedIndex> resolvedIndices, String... expectedIndices) {
        assertThat(resolvedIndices.size(), equalTo(expectedIndices.length));
        for (int k = 0; k < resolvedIndices.size(); k++) {
            ResolvedIndex resolvedIndex = resolvedIndices.get(k);
            Object[] indexInfo = findInfo(indices, expectedIndices[k]);
            if (indexInfo == null) {
                indexInfo = findBackingIndexInfo(dataStreams, expectedIndices[k]);
            }
            assertThat(indexInfo, notNullValue());
            assertThat(resolvedIndex.getName(), equalTo((String) indexInfo[0]));
            assertThat(resolvedIndex.getAliases(), is(((String[]) indexInfo[6])));
            assertThat(resolvedIndex.getAttributes(), is(flagsToAttributes(indexInfo)));
            assertThat(resolvedIndex.getDataStream(), equalTo((String) indexInfo[5]));
        }
    }

    private void validateAliases(List<ResolvedAlias> resolvedAliases, String... expectedAliases) {
        assertThat(resolvedAliases.size(), equalTo(expectedAliases.length));

        Map<String, Set<String>> aliasToIndicesMap = new HashMap<>();
        for (Object[] indexInfo : indices) {
            String[] aliases = (String[]) indexInfo[6];
            for (String alias : aliases) {
                Set<String> indicesSet = aliasToIndicesMap.get(alias);
                if (indicesSet == null) {
                    indicesSet = new HashSet<>();
                    aliasToIndicesMap.put(alias, indicesSet);
                }
                indicesSet.add((String) indexInfo[0]);
            }
        }
        for (int k = 0; k < resolvedAliases.size(); k++) {
            ResolvedAlias resolvedAlias = resolvedAliases.get(k);
            assertThat(resolvedAlias.getName(), equalTo(expectedAliases[k]));
            Set<String> aliasIndices = aliasToIndicesMap.get(resolvedAlias.getName());
            assertThat(aliasIndices, notNullValue());
            String[] expectedIndices = aliasIndices.toArray(Strings.EMPTY_ARRAY);
            Arrays.sort(expectedIndices);
            assertThat(resolvedAlias.getIndices(), is(expectedIndices));
        }
    }

    private void validateDataStreams(List<ResolvedDataStream> resolvedDataStreams, String... expectedDataStreams) {
        assertThat(resolvedDataStreams.size(), equalTo(expectedDataStreams.length));
        for (int k = 0; k < resolvedDataStreams.size(); k++) {
            ResolvedDataStream resolvedDataStream = resolvedDataStreams.get(k);
            Object[] dataStreamInfo = findInfo(dataStreams, expectedDataStreams[k]);
            assertThat(dataStreamInfo, notNullValue());
            assertThat(resolvedDataStream.getName(), equalTo((String) dataStreamInfo[0]));
            assertThat(resolvedDataStream.getTimestampField(), equalTo("@timestamp"));
            int numBackingIndices = (int) dataStreamInfo[1];
            List<String> expectedBackingIndices = new ArrayList<>();
            for (int m = 1; m <= numBackingIndices; m++) {
                expectedBackingIndices.add(DataStream.getDefaultBackingIndexName(resolvedDataStream.getName(), m, epochMillis));
            }
            assertThat(resolvedDataStream.getBackingIndices(), is((expectedBackingIndices.toArray(Strings.EMPTY_ARRAY))));
        }
    }

    Metadata.Builder buildMetadata(Object[][] dataStreams, Object[][] indices) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Object[] dsInfo : dataStreams) {
            String dataStreamName = (String) dsInfo[0];
            int numBackingIndices = (int) dsInfo[1];
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
                backingIndices.add(
                    createIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber, epochMillis), true)
                );
            }
            allIndices.addAll(backingIndices);

            DataStream ds = DataStreamTestHelper.newInstance(dataStreamName, backingIndices.stream().map(IndexMetadata::getIndex).toList());
            builder.put(ds);
        }

        for (Object[] indexInfo : indices) {
            String indexName = (String) indexInfo[0];
            String[] aliases = (String[]) indexInfo[6];
            boolean closed = (boolean) indexInfo[1];
            boolean hidden = (boolean) indexInfo[2];
            boolean system = (boolean) indexInfo[3];
            boolean frozen = (boolean) indexInfo[4];
            allIndices.add(createIndexMetadata(indexName, aliases, closed, hidden, system, frozen));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return builder;
    }

    private static IndexMetadata createIndexMetadata(
        String name,
        String[] aliases,
        boolean closed,
        boolean hidden,
        boolean system,
        boolean frozen
    ) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.hidden", hidden)
            .put("index.frozen", frozen);

        IndexMetadata.Builder indexBuilder = IndexMetadata.builder(name)
            .settings(settingsBuilder)
            .state(closed ? IndexMetadata.State.CLOSE : IndexMetadata.State.OPEN)
            .system(system)
            .numberOfShards(1)
            .numberOfReplicas(1);

        for (String alias : aliases) {
            indexBuilder.putAlias(AliasMetadata.builder(alias).build());
        }

        return indexBuilder.build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden) {
        return createIndexMetadata(name, Strings.EMPTY_ARRAY, false, true, false, false);
    }

    private static Object[] findInfo(Object[][] indexSource, String indexName) {
        for (Object[] info : indexSource) {
            if (info[0].equals(indexName)) {
                return info;
            }
        }
        return null;
    }

    private Object[] findBackingIndexInfo(Object[][] dataStreamSource, String indexName) {
        for (Object[] info : dataStreamSource) {
            String dataStreamName = (String) info[0];
            int generations = (int) info[1];
            for (int k = 1; k <= generations; k++) {
                if (DataStream.getDefaultBackingIndexName(dataStreamName, k, epochMillis).equals(indexName)) {
                    return new Object[] {
                        DataStream.getDefaultBackingIndexName(dataStreamName, k, epochMillis),
                        false,
                        true,
                        false,
                        false,
                        dataStreamName,
                        Strings.EMPTY_ARRAY };
                }
            }
        }
        return null;
    }

    private static String[] flagsToAttributes(Object[] indexInfo) {
        List<String> attributes = new ArrayList<>();
        attributes.add((boolean) indexInfo[1] ? "closed" : "open");
        if ((boolean) indexInfo[2]) {
            attributes.add("hidden");
        }
        if ((boolean) indexInfo[3]) {
            attributes.add("system");
        }
        if ((boolean) indexInfo[4]) {
            attributes.add("frozen");
        }
        attributes.sort(String::compareTo);
        return attributes.toArray(Strings.EMPTY_ARRAY);
    }

    private SystemIndices addSystemIndex(Metadata.Builder mdBuilder) {
        mdBuilder.put(indexBuilder(".test-system-1", SystemIndexDescriptor.DEFAULT_SETTINGS).state(IndexMetadata.State.OPEN).system(true))
            .put(
                indexBuilder(".test-net-new-system-1", SystemIndexDescriptor.DEFAULT_SETTINGS).state(IndexMetadata.State.OPEN).system(true)
            );
        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "test-system-feature",
                    "test system index",
                    List.of(
                        SystemIndexDescriptor.builder()
                            .setIndexPattern(".test-system*")
                            .setDescription("test-system-description")
                            .setType(SystemIndexDescriptor.Type.EXTERNAL_UNMANAGED)
                            .setAllowedElasticProductOrigins(List.of("test-system"))
                            .build(),
                        SystemIndexDescriptor.builder()
                            .setIndexPattern(".test-net-new-system*")
                            .setDescription("test-net-new-system-description")
                            .setType(SystemIndexDescriptor.Type.EXTERNAL_MANAGED)
                            .setAllowedElasticProductOrigins(List.of("test-net-new-system"))
                            .setNetNew()
                            .setSettings(Settings.EMPTY)
                            .setMappings("{ \"_doc\": { \"_meta\": { \"version\": \"8.0.0\" } } }")
                            .setIndexFormat(new SystemIndexDescriptor.IndexFormat(0, "1094029672"))
                            .setPrimaryIndex(".test-net-new-system-1")
                            .setVersionMetaKey("version")
                            .setOrigin("system")
                            .build()
                    )
                )
            )
        );
        return systemIndices;
    }

    private static IndexMetadata.Builder indexBuilder(String index, Settings additionalSettings) {
        return IndexMetadata.builder(index).settings(indexSettings(Version.CURRENT, 1, 0).put(additionalSettings));
    }

    private ThreadContext createThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
