/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.health.node.selection.HealthNodeTaskParams;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.alias.RandomAliasActionsGenerator;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasks;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_API;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.elasticsearch.cluster.metadata.ProjectMetadata.Builder.assertDataStreams;
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataTests extends ESTestCase {

    public void testFindAliases() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias1").build())
                    .putAlias(AliasMetadata.builder("alias2").build())
            )
            .put(
                IndexMetadata.builder("index2")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias2").build())
                    .putAlias(AliasMetadata.builder("alias3").build())
            )
            .build();

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
            Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAliases(request.aliases(), Strings.EMPTY_ARRAY);
            assertThat(aliases, anEmptyMap());
        }
        {
            final GetAliasesRequest request;
            if (randomBoolean()) {
                request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
            } else {
                request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, randomFrom("alias1", "alias2"));
                // replacing with empty aliases behaves as if aliases were unspecified at request building
                request.replaceAliases(Strings.EMPTY_ARRAY);
            }
            Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
        }
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "alias*");
            Map<String, List<AliasMetadata>> aliases = metadata.getProject()
                .findAliases(request.aliases(), new String[] { "index", "index2" });
            assertThat(aliases, aMapWithSize(2));
            List<AliasMetadata> indexAliasMetadataList = aliases.get("index");
            assertThat(indexAliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
            List<AliasMetadata> index2AliasMetadataList = aliases.get("index2");
            assertThat(index2AliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias2", "alias3")));
        }
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "alias1");
            Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1")));
        }
        {
            Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAllAliases(new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
        }
        {
            Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAllAliases(Strings.EMPTY_ARRAY);
            assertThat(aliases, anEmptyMap());
        }
    }

    public void testFindDataStreamAliases() {
        Metadata.Builder builder = Metadata.builder();

        addDataStream("d1", builder);
        addDataStream("d2", builder);
        addDataStream("d3", builder);
        addDataStream("d4", builder);

        builder.put("alias1", "d1", null, null);
        builder.put("alias2", "d2", null, null);
        builder.put("alias2-part2", "d2", null, null);

        Metadata metadata = builder.build();

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
            Map<String, List<DataStreamAlias>> aliases = metadata.getProject()
                .findDataStreamAliases(request.aliases(), Strings.EMPTY_ARRAY);
            assertThat(aliases, anEmptyMap());
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("alias1");
            Map<String, List<DataStreamAlias>> aliases = metadata.getProject()
                .findDataStreamAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, anEmptyMap());
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("alias1");
            Map<String, List<DataStreamAlias>> aliases = metadata.getProject()
                .findDataStreamAliases(request.aliases(), new String[] { "index", "d1", "d2" });
            assertEquals(1, aliases.size());
            List<DataStreamAlias> found = aliases.get("d1");
            assertThat(found, transformedItemsMatch(DataStreamAlias::getAlias, contains("alias1")));
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("ali*");
            Map<String, List<DataStreamAlias>> aliases = metadata.getProject()
                .findDataStreamAliases(request.aliases(), new String[] { "index", "d2" });
            assertEquals(1, aliases.size());
            List<DataStreamAlias> found = aliases.get("d2");
            assertThat(found, transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
        }

        // test exclusion
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("*");
            Map<String, List<DataStreamAlias>> aliases = metadata.getProject()
                .findDataStreamAliases(request.aliases(), new String[] { "index", "d1", "d2", "d3", "d4" });
            assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
            assertThat(aliases.get("d1"), transformedItemsMatch(DataStreamAlias::getAlias, contains("alias1")));

            request.aliases("*", "-alias1");
            aliases = metadata.getProject().findDataStreamAliases(request.aliases(), new String[] { "index", "d1", "d2", "d3", "d4" });
            assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
            assertNull(aliases.get("d1"));
        }
    }

    public void testDataStreamAliasesByDataStream() {
        Metadata.Builder builder = Metadata.builder();

        addDataStream("d1", builder);
        addDataStream("d2", builder);
        addDataStream("d3", builder);
        addDataStream("d4", builder);

        builder.put("alias1", "d1", null, null);
        builder.put("alias2", "d2", null, null);
        builder.put("alias2-part2", "d2", null, null);

        Metadata metadata = builder.build();

        var aliases = metadata.getProject().dataStreamAliasesByDataStream();

        assertTrue(aliases.containsKey("d1"));
        assertTrue(aliases.containsKey("d2"));
        assertFalse(aliases.containsKey("d3"));
        assertFalse(aliases.containsKey("d4"));

        assertEquals(1, aliases.get("d1").size());
        assertEquals(2, aliases.get("d2").size());

        assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
    }

    public void testFindAliasWithExclusion() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias1").build())
                    .putAlias(AliasMetadata.builder("alias2").build())
            )
            .put(
                IndexMetadata.builder("index2")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("alias1").build())
                    .putAlias(AliasMetadata.builder("alias3").build())
            )
            .build();
        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("*", "-alias1");
        Map<String, List<AliasMetadata>> aliases = metadata.getProject().findAliases(request.aliases(), new String[] { "index", "index2" });
        assertThat(aliases.get("index"), transformedItemsMatch(AliasMetadata::alias, contains("alias2")));
        assertThat(aliases.get("index2"), transformedItemsMatch(AliasMetadata::alias, contains("alias3")));
    }

    public void testFindDataStreams() {
        final int numIndices = randomIntBetween(2, 5);
        final int numBackingIndices = randomIntBetween(2, 5);
        final String dataStreamName = "my-data-stream";
        CreateIndexResult result = createIndices(numIndices, numBackingIndices, dataStreamName);

        List<Index> allIndices = new ArrayList<>(result.indices);
        allIndices.addAll(result.backingIndices);
        String[] concreteIndices = allIndices.stream().map(Index::getName).toArray(String[]::new);
        Map<String, DataStream> dataStreams = result.metadata.getProject().findDataStreams(concreteIndices);
        assertThat(dataStreams, aMapWithSize(numBackingIndices));
        for (Index backingIndex : result.backingIndices) {
            assertThat(dataStreams, hasKey(backingIndex.getName()));
            assertThat(dataStreams.get(backingIndex.getName()).getName(), equalTo(dataStreamName));
        }
    }

    public void testFindAliasWithExclusionAndOverride() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("index")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("aa").build())
                    .putAlias(AliasMetadata.builder("ab").build())
                    .putAlias(AliasMetadata.builder("bb").build())
            )
            .build();
        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("a*", "-*b", "b*");
        List<AliasMetadata> aliases = metadata.getProject().findAliases(request.aliases(), new String[] { "index" }).get("index");
        assertThat(aliases, transformedItemsMatch(AliasMetadata::alias, contains("aa", "bb")));
    }

    public void testAliasCollidingWithAnExistingIndex() {
        int indexCount = randomIntBetween(10, 100);
        Set<String> indices = Sets.newHashSetWithExpectedSize(indexCount);
        for (int i = 0; i < indexCount; i++) {
            indices.add(randomAlphaOfLength(10));
        }
        Map<String, Set<String>> aliasToIndices = new HashMap<>();
        for (String alias : randomSubsetOf(randomIntBetween(1, 10), indices)) {
            Set<String> indicesInAlias;
            do {
                indicesInAlias = new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices));
                indicesInAlias.remove(alias);
            } while (indicesInAlias.isEmpty());
            aliasToIndices.put(alias, indicesInAlias);
        }
        int properAliases = randomIntBetween(0, 3);
        for (int i = 0; i < properAliases; i++) {
            aliasToIndices.put(randomAlphaOfLength(5), new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (String index : indices) {
            IndexMetadata.Builder indexBuilder = IndexMetadata.builder(index)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0);
            aliasToIndices.forEach((key, value) -> {
                if (value.contains(index)) {
                    indexBuilder.putAlias(AliasMetadata.builder(key).build());
                }
            });
            metadataBuilder.put(indexBuilder);
        }

        Exception e = expectThrows(IllegalStateException.class, metadataBuilder::build);
        assertThat(e.getMessage(), startsWith("index, alias, and data stream names need to be unique"));
    }

    public void testValidateAliasWriteOnly() {
        String alias = randomAlphaOfLength(5);
        String indexA = randomAlphaOfLength(6);
        String indexB = randomAlphaOfLength(7);
        Boolean aWriteIndex = randomBoolean() ? null : randomBoolean();
        Boolean bWriteIndex;
        if (Boolean.TRUE.equals(aWriteIndex)) {
            bWriteIndex = randomFrom(Boolean.FALSE, null);
        } else {
            bWriteIndex = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        }
        // when only one index/alias pair exist
        Metadata metadata = Metadata.builder().put(buildIndexMetadata(indexA, alias, aWriteIndex)).build();

        // when alias points to two indices, but valid
        // one of the following combinations: [(null, null), (null, true), (null, false), (false, false)]
        Metadata.builder(metadata).put(buildIndexMetadata(indexB, alias, bWriteIndex)).build();

        // when too many write indices
        Exception exception = expectThrows(IllegalStateException.class, () -> {
            IndexMetadata.Builder metaA = buildIndexMetadata(indexA, alias, true);
            IndexMetadata.Builder metaB = buildIndexMetadata(indexB, alias, true);
            Metadata.builder().put(metaA).put(metaB).build();
        });
        assertThat(exception.getMessage(), startsWith("alias [" + alias + "] has more than one write index ["));
    }

    public void testValidateHiddenAliasConsistency() {
        String alias = randomAlphaOfLength(5);
        String indexA = randomAlphaOfLength(6);
        String indexB = randomAlphaOfLength(7);

        {
            Exception ex = expectThrows(
                IllegalStateException.class,
                () -> buildMetadataWithHiddenIndexMix(alias, indexA, true, indexB, randomFrom(false, null)).build()
            );
            assertThat(ex.getMessage(), containsString("has is_hidden set to true on indices"));
        }

        {
            Exception ex = expectThrows(
                IllegalStateException.class,
                () -> buildMetadataWithHiddenIndexMix(alias, indexA, randomFrom(false, null), indexB, true).build()
            );
            assertThat(ex.getMessage(), containsString("has is_hidden set to true on indices"));
        }
    }

    private Metadata.Builder buildMetadataWithHiddenIndexMix(
        String aliasName,
        String indexAName,
        Boolean indexAHidden,
        String indexBName,
        Boolean indexBHidden
    ) {
        IndexMetadata.Builder indexAMeta = IndexMetadata.builder(indexAName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).isHidden(indexAHidden).build());
        IndexMetadata.Builder indexBMeta = IndexMetadata.builder(indexBName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).isHidden(indexBHidden).build());
        return Metadata.builder().put(indexAMeta).put(indexBMeta);
    }

    public void testResolveIndexRouting() {
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build())
            .putAlias(AliasMetadata.builder("alias1").routing("1").build())
            .putAlias(AliasMetadata.builder("alias2").routing("1,2").build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // no alias, no index
        assertNull(metadata.getProject().resolveIndexRouting(null, null));
        assertEquals(metadata.getProject().resolveIndexRouting("0", null), "0");

        // index, no alias
        assertNull(metadata.getProject().resolveIndexRouting(null, "index"));
        assertEquals(metadata.getProject().resolveIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertNull(metadata.getProject().resolveIndexRouting(null, "alias0"));
        assertEquals(metadata.getProject().resolveIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metadata.getProject().resolveIndexRouting(null, "alias1"), "1");
        Exception ex = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveIndexRouting("0", "alias1"));
        assertThat(
            ex.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation")
        );

        // alias with invalid index routing.
        ex = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveIndexRouting(null, "alias2"));
        assertThat(
            ex.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        ex = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveIndexRouting("1", "alias2"));
        assertThat(
            ex.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        IndexMetadata.Builder builder2 = IndexMetadata.builder("index2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build());
        Metadata metadataTwoIndices = Metadata.builder(metadata).put(builder2).build();

        // alias with multiple indices
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> metadataTwoIndices.getProject().resolveIndexRouting("1", "alias0")
        );
        assertThat(exception.getMessage(), startsWith("Alias [alias0] has more than one index associated with it"));
    }

    public void testResolveWriteIndexRouting() {
        AliasMetadata.Builder aliasZeroBuilder = AliasMetadata.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(true);
        }
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilder.build())
            .putAlias(AliasMetadata.builder("alias1").routing("1").build())
            .putAlias(AliasMetadata.builder("alias2").routing("1,2").build())
            .putAlias(AliasMetadata.builder("alias3").writeIndex(false).build())
            .putAlias(AliasMetadata.builder("alias4").routing("1,2").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // no alias, no index
        assertNull(metadata.getProject().resolveWriteIndexRouting(null, null));
        assertEquals(metadata.getProject().resolveWriteIndexRouting("0", null), "0");

        // index, no alias
        assertNull(metadata.getProject().resolveWriteIndexRouting(null, "index"));
        assertEquals(metadata.getProject().resolveWriteIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertNull(metadata.getProject().resolveWriteIndexRouting(null, "alias0"));
        assertEquals(metadata.getProject().resolveWriteIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metadata.getProject().resolveWriteIndexRouting(null, "alias1"), "1");
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> metadata.getProject().resolveWriteIndexRouting("0", "alias1")
        );
        assertThat(
            exception.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation")
        );

        // alias with invalid index routing.
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveWriteIndexRouting(null, "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveWriteIndexRouting("1", "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> metadata.getProject().resolveWriteIndexRouting(randomFrom("1", null), "alias4")
        );
        assertThat(
            exception.getMessage(),
            is("index/alias [alias4] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        // alias with no write index
        exception = expectThrows(IllegalArgumentException.class, () -> metadata.getProject().resolveWriteIndexRouting("1", "alias3"));
        assertThat(exception.getMessage(), is("alias [alias3] does not have a write index"));

        // aliases with multiple indices
        AliasMetadata.Builder aliasZeroBuilderTwo = AliasMetadata.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(false);
        }
        IndexMetadata.Builder builder2 = IndexMetadata.builder("index2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilderTwo.build())
            .putAlias(AliasMetadata.builder("alias1").routing("0").writeIndex(true).build())
            .putAlias(AliasMetadata.builder("alias2").writeIndex(true).build());
        Metadata metadataTwoIndices = Metadata.builder(metadata).put(builder2).build();

        // verify that new write index is used
        assertThat("0", equalTo(metadataTwoIndices.getProject().resolveWriteIndexRouting("0", "alias1")));
    }

    public void testUnknownFieldClusterMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("meta-data").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> Metadata.Builder.fromXContent(parser));
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testUnknownFieldIndexMetadata() throws IOException {
        BytesReference metadata = BytesReference.bytes(
            JsonXContent.contentBuilder().startObject().startObject("index_name").field("random", "value").endObject().endObject()
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> IndexMetadata.Builder.fromXContent(parser));
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testMetadataGlobalStateChangesOnIndexDeletions() {
        final var projectId = randomProjectIdOrDefault();
        IndexGraveyard.Builder builder = IndexGraveyard.builder();
        builder.addTombstone(new Index("idx1", UUIDs.randomBase64UUID()));
        final Metadata metadata1 = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(builder.build())).build();
        builder = IndexGraveyard.builder(metadata1.getProject(projectId).indexGraveyard());
        builder.addTombstone(new Index("idx2", UUIDs.randomBase64UUID()));
        final Metadata metadata2 = Metadata.builder(metadata1)
            .put(ProjectMetadata.builder(metadata1.getProject(projectId)).indexGraveyard(builder.build()))
            .build();
        assertFalse("metadata not equal after adding index deletions", Metadata.isGlobalStateEquals(metadata1, metadata2));
        final Metadata metadata3 = Metadata.builder(metadata2).build();
        assertTrue("metadata equal when not adding index deletions", Metadata.isGlobalStateEquals(metadata2, metadata3));
    }

    public void testXContentWithIndexGraveyard() throws IOException {
        @FixForMultiProject // XContent serialization and parsing with a random project ID currently only works when serializing in MP mode
        final var projectId = ProjectId.DEFAULT;
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(graveyard)).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, originalMeta);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(
                fromXContentMeta.getProject(projectId).indexGraveyard(),
                equalTo(originalMeta.getProject(projectId).indexGraveyard())
            );
        }
    }

    public void testXContentClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, originalMeta);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final Metadata fromXContentMeta = Metadata.fromXContent(parser);
            assertThat(fromXContentMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
            assertThat(fromXContentMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
        }
    }

    public void testSerializationClusterUUID() throws IOException {
        final Metadata originalMeta = Metadata.builder()
            .clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean())
            .build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
        assertThat(fromStreamMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
    }

    public void testMetadataGlobalStateChangesOnClusterUUIDChanges() {
        final Metadata metadata1 = Metadata.builder().clusterUUID(UUIDs.randomBase64UUID()).clusterUUIDCommitted(randomBoolean()).build();
        final Metadata metadata2 = Metadata.builder(metadata1).clusterUUID(UUIDs.randomBase64UUID()).build();
        final Metadata metadata3 = Metadata.builder(metadata1).clusterUUIDCommitted(metadata1.clusterUUIDCommitted() == false).build();
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata3));
        final Metadata metadata4 = Metadata.builder(metadata2).clusterUUID(metadata1.clusterUUID()).build();
        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata4));
    }

    public void testMetadataGlobalStateChangesOnProjectChanges() {
        final Metadata metadata1 = Metadata.builder().build();
        final Metadata metadata2 = Metadata.builder(metadata1).put(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
        final Metadata metadata3 = Metadata.builder(metadata1)
            .put(
                ProjectMetadata.builder(randomUniqueProjectId())
                    .put(IndexMetadata.builder("some-index").settings(indexSettings(IndexVersion.current(), 1, 1)))
                    .build()
            )
            .build();
        // A project with a ProjectCustom.
        final Metadata metadata4 = Metadata.builder(metadata1)
            .put(
                ProjectMetadata.builder(randomUniqueProjectId())
                    .put("template", new ComponentTemplate(new Template(null, null, null), null, null))
                    .build()
            )
            .build();
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
        assertTrue(Metadata.isGlobalStateEquals(metadata2, Metadata.builder(metadata1).projectMetadata(metadata2.projects()).build()));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata3));
        assertTrue(Metadata.isGlobalStateEquals(metadata3, Metadata.builder(metadata1).projectMetadata(metadata3.projects()).build()));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata4));
        assertTrue(Metadata.isGlobalStateEquals(metadata4, Metadata.builder(metadata1).projectMetadata(metadata4.projects()).build()));
    }

    private static CoordinationMetadata.VotingConfiguration randomVotingConfig() {
        return new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    private Set<VotingConfigExclusion> randomVotingConfigExclusions() {
        final int size = randomIntBetween(0, 10);
        final Set<VotingConfigExclusion> nodes = Sets.newHashSetWithExpectedSize(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
    }

    public void testXContentWithCoordinationMetadata() throws IOException {
        CoordinationMetadata originalMeta = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );

        Metadata metadata = Metadata.builder().coordinationMetadata(originalMeta).build();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetadata fromXContentMeta = Metadata.fromXContent(parser).coordinationMetadata();
            assertThat(fromXContentMeta, equalTo(originalMeta));
        }
    }

    public void testParseXContentFormatBeforeMultiProject() throws IOException {
        final String json = org.elasticsearch.core.Strings.format("""
            {
              "meta-data": {
                "version": 54321,
                "cluster_uuid":"aba1aa1ababbbaabaabaab",
                "cluster_uuid_committed":false,
                "cluster_coordination":{
                  "term":1,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions":[]
                },
                "templates":{
                  "template":{
                    "order":0,
                    "index_patterns":["index-*"],
                    "settings":{
                      "something":true
                    },
                    "mappings":{ },
                    "aliases":{ }
                  }
                },
                "persistent_tasks": {
                  "last_allocation_id": 1,
                  "tasks": [
                    {
                      "id": "health-node",
                      "task":{ "health-node": {"params":{}} }
                    }
                  ]
                },
                "index-graveyard":{
                  "tombstones":[{
                    "index":{
                      "index_name":"old-index",
                      "index_uuid":"index_index_index_1234"
                    },
                    "delete_date_in_millis":1717170000000
                  }]
                },
                "desired_nodes":{
                  "latest": {
                    "history_id": "test",
                    "version": 1,
                    "nodes": [{
                      "settings":{ "node":{"name":"node-dn1"} },
                      "processors": 3.5,
                      "memory": "32gb",
                      "storage": "256gb",
                      "status": 0
                    }]
                  }
                },
                "component_template":{
                  "component_template":{
                    "sample-template":{
                      "template":{
                        "mappings":"REZMAKtWKijKL0gtKslMLVayqlaKCndxiwAxSioLUpWslLJTK8vzi1KUamtrAQ=="
                      },
                      "_meta":{
                        "awesome":true
                      },
                      "deprecated":false
                    }
                  }
                },
                "repositories": {
                  "my-repo": {
                    "type": "fs",
                    "uuid": "_my-repo-uuid_",
                    "settings": {
                      "location": "backup"
                    },
                    "generation": 42,
                    "pending_generation": 42
                  }
                },
                "reserved_state":{ }
              }
            }
            """, IndexVersion.current(), IndexVersion.current());

        final var metadata = fromJsonXContentStringWithPersistentTasks(json);

        assertThat(metadata, notNullValue());
        assertThat(metadata.clusterUUID(), is("aba1aa1ababbbaabaabaab"));
        assertThat(metadata.customs().keySet(), containsInAnyOrder("desired_nodes", "cluster_persistent_tasks"));
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(metadata);
        assertThat(clusterTasks.tasks(), hasSize(1));
        assertThat(
            clusterTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getTaskName).toList(),
            containsInAnyOrder("health-node")
        );
        assertThat(
            metadata.getProject().customs().keySet(),
            containsInAnyOrder("persistent_tasks", "index-graveyard", "component_template", "repositories")
        );
        assertThat(metadata.customs(), not(hasKey("repositories")));
        final var repositoriesMetadata = RepositoriesMetadata.get(metadata.getProject(ProjectId.DEFAULT));
        assertThat(
            repositoriesMetadata.repositories(),
            equalTo(
                List.of(
                    new RepositoryMetadata("my-repo", "_my-repo-uuid_", "fs", Settings.builder().put("location", "backup").build(), 42, 42)
                )
            )
        );
    }

    public void testParseXContentFormatBeforeRepositoriesMetadataMigration() throws IOException {
        final String json = org.elasticsearch.core.Strings.format("""
            {
              "meta-data": {
                "version": 54321,
                "cluster_uuid":"aba1aa1ababbbaabaabaab",
                "cluster_uuid_committed":false,
                "cluster_coordination":{
                  "term":1,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions":[]
                },
                "projects" : [
                  {
                    "id" : "default",
                    "templates" : {
                      "template" : {
                        "order" : 0,
                        "index_patterns" : [
                          "pattern1",
                          "pattern2"
                        ],
                        "mappings" : {
                          "key1" : { }
                        },
                        "aliases" : { }
                      }
                    },
                    "index-graveyard" : {
                      "tombstones" : [ ]
                    },
                    "reserved_state" : { }
                  },
                  {
                    "id" : "another_project",
                    "templates" : {
                      "template" : {
                        "order" : 0,
                        "index_patterns" : [
                          "pattern1",
                          "pattern2"
                        ],
                        "mappings" : {
                          "key1" : { }
                        },
                        "aliases" : { }
                      }
                    },
                    "index-graveyard" : {
                      "tombstones" : [ ]
                    },
                    "reserved_state" : { }
                  }
                ],
                "repositories": {
                  "my-repo": {
                    "type": "fs",
                    "uuid": "_my-repo-uuid_",
                    "settings": {
                      "location": "backup"
                    },
                    "generation": 42,
                    "pending_generation": 42
                  }
                },
                "reserved_state":{ }
              }
            }
            """, IndexVersion.current(), IndexVersion.current());

        final Metadata metadata = fromJsonXContentStringWithPersistentTasks(json);
        assertThat(metadata, notNullValue());
        assertThat(metadata.clusterUUID(), is("aba1aa1ababbbaabaabaab"));

        assertThat(metadata.projects().keySet(), containsInAnyOrder(ProjectId.fromId("default"), ProjectId.fromId("another_project")));
        assertThat(metadata.customs(), not(hasKey("repositories")));
        final var repositoriesMetadata = RepositoriesMetadata.get(metadata.getProject(ProjectId.DEFAULT));
        assertThat(
            repositoriesMetadata.repositories(),
            equalTo(
                List.of(
                    new RepositoryMetadata("my-repo", "_my-repo-uuid_", "fs", Settings.builder().put("location", "backup").build(), 42, 42)
                )
            )
        );
        assertThat(metadata.getProject(ProjectId.fromId("another_project")).customs(), not(hasKey("repositories")));
    }

    private Metadata fromJsonXContentStringWithPersistentTasks(String json) throws IOException {
        List<NamedXContentRegistry.Entry> registry = new ArrayList<>();
        registry.addAll(ClusterModule.getNamedXWriteables());
        registry.addAll(IndicesModule.getNamedXContents());
        registry.addAll(HealthNodeTaskExecutor.getNamedXContentParsers());

        final var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
        final var healthNodeTaskExecutor = HealthNodeTaskExecutor.create(
            clusterService,
            mock(PersistentTasksService.class),
            Settings.EMPTY,
            ClusterSettings.createBuiltInClusterSettings()
        );
        new PersistentTasksExecutorRegistry(List.of(healthNodeTaskExecutor));

        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(registry));
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(config, json)) {
            return Metadata.fromXContent(parser);
        }
    }

    public void testGlobalStateEqualsCoordinationMetadata() {
        CoordinationMetadata coordinationMetadata1 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata1 = Metadata.builder().coordinationMetadata(coordinationMetadata1).build();
        CoordinationMetadata coordinationMetadata2 = new CoordinationMetadata(
            randomNonNegativeLong(),
            randomVotingConfig(),
            randomVotingConfig(),
            randomVotingConfigExclusions()
        );
        Metadata metadata2 = Metadata.builder().coordinationMetadata(coordinationMetadata2).build();

        assertTrue(Metadata.isGlobalStateEquals(metadata1, metadata1));
        assertFalse(Metadata.isGlobalStateEquals(metadata1, metadata2));
    }

    public void testSerializationWithIndexGraveyard() throws IOException {
        final var projectId = randomProjectIdOrDefault();
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final Metadata originalMeta = Metadata.builder().put(ProjectMetadata.builder(projectId).indexGraveyard(graveyard)).build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.getProject(projectId).indexGraveyard(), equalTo(originalMeta.getProject(projectId).indexGraveyard()));
    }

    public void testFindMappings() throws IOException {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(FIND_MAPPINGS_TEST_ITEM))
            .put(IndexMetadata.builder("index2").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(FIND_MAPPINGS_TEST_ITEM))
            .build();

        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(Strings.EMPTY_ARRAY, MapperPlugin.NOOP_FIELD_FILTER, onNextIndexCalls::incrementAndGet);
            assertThat(mappings, anEmptyMap());
            assertThat(onNextIndexCalls.get(), equalTo(0));
        }
        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(new String[] { "index1" }, MapperPlugin.NOOP_FIELD_FILTER, onNextIndexCalls::incrementAndGet);
            assertThat(mappings, aMapWithSize(1));
            assertIndexMappingsNotFiltered(mappings, "index1");
            assertThat(onNextIndexCalls.get(), equalTo(1));
        }
        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(new String[] { "index1", "index2" }, MapperPlugin.NOOP_FIELD_FILTER, onNextIndexCalls::incrementAndGet);
            assertThat(mappings, aMapWithSize(2));
            assertIndexMappingsNotFiltered(mappings, "index1");
            assertIndexMappingsNotFiltered(mappings, "index2");
            assertThat(onNextIndexCalls.get(), equalTo(2));
        }
    }

    public void testFindMappingsNoOpFilters() throws IOException {
        MappingMetadata originalMappingMetadata = new MappingMetadata(
            "_doc",
            XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, true)
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(originalMappingMetadata))
            .build();

        {
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(new String[] { "index1" }, MapperPlugin.NOOP_FIELD_FILTER, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata mappingMetadata = mappings.get("index1");
            assertSame(originalMappingMetadata, mappingMetadata);
        }
        {
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(new String[] { "index1" }, index -> field -> randomBoolean(), Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
            MappingMetadata mappingMetadata = mappings.get("index1");
            assertNotSame(originalMappingMetadata, mappingMetadata);
        }
    }

    @SuppressWarnings("unchecked")
    public void testFindMappingsWithFilters() throws IOException {
        String mapping = FIND_MAPPINGS_TEST_ITEM;
        if (randomBoolean()) {
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, false);
            Map<String, Object> doc = (Map<String, Object>) stringObjectMap.get("_doc");
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(doc);
                mapping = Strings.toString(builder);
            }
        }

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .put(IndexMetadata.builder("index2").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .put(IndexMetadata.builder("index3").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .build();

        {
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(new String[] { "index1", "index2", "index3" }, index -> {
                    if (index.equals("index1")) {
                        return field -> field.startsWith("name.") == false
                            && field.startsWith("properties.key.") == false
                            && field.equals("age") == false
                            && field.equals("address.location") == false;
                    }
                    if (index.equals("index2")) {
                        return Predicates.never();
                    }
                    return FieldPredicate.ACCEPT_ALL;
                }, Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);

            assertIndexMappingsNoFields(mappings, "index2");
            assertIndexMappingsNotFiltered(mappings, "index3");

            MappingMetadata docMapping = mappings.get("index1");
            assertNotNull(docMapping);

            Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
            assertThat(sourceAsMap.keySet(), containsInAnyOrder("properties", "_routing", "_source"));

            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertThat(typeProperties.keySet(), containsInAnyOrder("name", "address", "birth", "ip", "suggest", "properties"));

            Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
            assertThat(name.keySet(), containsInAnyOrder("properties"));
            Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
            assertThat(nameProperties, anEmptyMap());

            Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
            assertThat(address.keySet(), containsInAnyOrder("type", "properties"));
            Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
            assertThat(addressProperties.keySet(), containsInAnyOrder("street", "area"));
            assertLeafs(addressProperties, "street", "area");

            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertThat(properties.keySet(), containsInAnyOrder("type", "properties"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertThat(propertiesProperties.keySet(), containsInAnyOrder("key", "value"));
            assertLeafs(propertiesProperties, "key");
            assertMultiField(propertiesProperties, "value", "keyword");
        }

        {
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(
                    new String[] { "index1", "index2", "index3" },
                    index -> field -> (index.equals("index3") && field.endsWith("keyword")),
                    Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP
                );

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index2");
            MappingMetadata mappingMetadata = mappings.get("index3");
            Map<String, Object> sourceAsMap = mappingMetadata.getSourceAsMap();
            assertThat(sourceAsMap.keySet(), containsInAnyOrder("_routing", "_source", "properties"));
            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertThat(typeProperties.keySet(), containsInAnyOrder("properties"));
            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertThat(properties.keySet(), containsInAnyOrder("type", "properties"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertThat(propertiesProperties.keySet(), containsInAnyOrder("key", "value"));
            Map<String, Object> key = (Map<String, Object>) propertiesProperties.get("key");
            assertThat(key.keySet(), containsInAnyOrder("properties"));
            Map<String, Object> keyProperties = (Map<String, Object>) key.get("properties");
            assertThat(keyProperties.keySet(), containsInAnyOrder("keyword"));
            assertLeafs(keyProperties, "keyword");
            Map<String, Object> value = (Map<String, Object>) propertiesProperties.get("value");
            assertThat(value.keySet(), containsInAnyOrder("properties"));
            Map<String, Object> valueProperties = (Map<String, Object>) value.get("properties");
            assertThat(valueProperties.keySet(), containsInAnyOrder("keyword"));
            assertLeafs(valueProperties, "keyword");
        }

        {
            Map<String, MappingMetadata> mappings = metadata.getProject()
                .findMappings(
                    new String[] { "index1", "index2", "index3" },
                    index -> field -> (index.equals("index2")),
                    Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP
                );

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index3");
            assertIndexMappingsNotFiltered(mappings, "index2");
        }
    }

    public void testOldestIndexComputation() {
        Metadata metadata = buildIndicesWithVersions(
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            IndexVersion.fromId(IndexVersion.current().id() + 1)
        ).build();

        assertEquals(IndexVersions.MINIMUM_COMPATIBLE, metadata.getProject().oldestIndexVersion());

        Metadata.Builder b = Metadata.builder();
        assertEquals(IndexVersion.current(), b.build().getProject().oldestIndexVersion());

        Throwable ex = expectThrows(
            IllegalArgumentException.class,
            () -> buildIndicesWithVersions(
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersions.ZERO,
                IndexVersion.fromId(IndexVersion.current().id() + 1)
            ).build()
        );

        assertEquals("[index.version.created] is not present in the index settings for index with UUID [null]", ex.getMessage());
    }

    private Metadata.Builder buildIndicesWithVersions(IndexVersion... indexVersions) {
        int lastIndexNum = randomIntBetween(9, 50);
        Metadata.Builder b = Metadata.builder();
        for (IndexVersion indexVersion : indexVersions) {
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("index", lastIndexNum))
                .settings(settings(indexVersion))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            lastIndexNum = randomIntBetween(lastIndexNum + 1, lastIndexNum + 50);
        }

        return b;
    }

    private static IndexMetadata.Builder buildIndexMetadata(String name, String alias, Boolean writeIndex) {
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()))
            .creationDate(randomNonNegativeLong())
            .putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex))
            .numberOfShards(1)
            .numberOfReplicas(0);
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNoFields(Map<String, MappingMetadata> mappings, String index) {
        MappingMetadata docMapping = mappings.get(index);
        assertNotNull(docMapping);
        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertThat(sourceAsMap.keySet(), containsInAnyOrder("_routing", "_source", "properties"));
        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertThat(typeProperties, anEmptyMap());
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNotFiltered(Map<String, MappingMetadata> mappings, String index) {
        MappingMetadata docMapping = mappings.get(index);
        assertNotNull(docMapping);

        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertThat(sourceAsMap.keySet(), containsInAnyOrder("_routing", "_source", "properties"));

        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertThat(typeProperties.keySet(), containsInAnyOrder("name", "address", "birth", "age", "ip", "suggest", "properties"));

        Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
        assertThat(name.keySet(), containsInAnyOrder("properties"));
        Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
        assertThat(nameProperties.keySet(), containsInAnyOrder("first", "last"));
        assertLeafs(nameProperties, "first", "last");

        Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
        assertThat(address.keySet(), containsInAnyOrder("type", "properties"));
        Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
        assertThat(addressProperties.keySet(), containsInAnyOrder("street", "location", "area"));
        assertLeafs(addressProperties, "street", "location", "area");

        Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
        assertThat(properties.keySet(), containsInAnyOrder("type", "properties"));
        Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
        assertThat(propertiesProperties.keySet(), containsInAnyOrder("key", "value"));
        assertMultiField(propertiesProperties, "key", "keyword");
        assertMultiField(propertiesProperties, "value", "keyword");
    }

    @SuppressWarnings("unchecked")
    public static void assertLeafs(Map<String, Object> properties, String... fields) {
        assertThat(properties.keySet(), hasItems(fields));
        for (String field : fields) {
            Map<String, Object> fieldProp = (Map<String, Object>) properties.get(field);
            assertThat(fieldProp, not(hasKey("properties")));
            assertThat(fieldProp, not(hasKey("fields")));
        }
    }

    public static void assertMultiField(Map<String, Object> properties, String field, String... subFields) {
        assertThat(properties, hasKey(field));
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldProp = (Map<String, Object>) properties.get(field);
        assertThat(fieldProp, hasKey("fields"));
        @SuppressWarnings("unchecked")
        Map<String, Object> subFieldsDef = (Map<String, Object>) fieldProp.get("fields");
        assertLeafs(subFieldsDef, subFields);
    }

    private static final String FIND_MAPPINGS_TEST_ITEM = """
        {
          "_doc": {
              "_routing": {
                "required":true
              },      "_source": {
                "enabled":false
              },      "properties": {
                "name": {
                  "properties": {
                    "first": {
                      "type": "keyword"
                    },
                    "last": {
                      "type": "keyword"
                    }
                  }
                },
                "birth": {
                  "type": "date"
                },
                "age": {
                  "type": "integer"
                },
                "ip": {
                  "type": "ip"
                },
                "suggest" : {
                  "type": "completion"
                },
                "address": {
                  "type": "object",
                  "properties": {
                    "street": {
                      "type": "keyword"
                    },
                    "location": {
                      "type": "geo_point"
                    },
                    "area": {
                      "type": "geo_shape", \s
                      "tree": "quadtree",
                      "precision": "1m"
                    }
                  }
                },
                "properties": {
                  "type": "nested",
                  "properties": {
                    "key" : {
                      "type": "text",
                      "fields": {
                        "keyword" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "value" : {
                      "type": "text",
                      "fields": {
                        "keyword" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }""";

    public void testTransientSettingsOverridePersistentSettings() {
        final Setting<String> setting = Setting.simpleString("key");
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(setting.getKey(), "persistent-value").build())
            .transientSettings(Settings.builder().put(setting.getKey(), "transient-value").build())
            .build();
        assertThat(setting.get(metadata.settings()), equalTo("transient-value"));
    }

    public void testBuilderRejectsNullCustom() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        assertThat(
            expectThrows(NullPointerException.class, () -> builder.putCustom(key, (Metadata.ClusterCustom) null)).getMessage(),
            containsString(key)
        );
        assertThat(expectThrows(NullPointerException.class, () -> builder.putProjectCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final Metadata.Builder builder = Metadata.builder();
        final String key = randomAlphaOfLength(10);
        {
            final Map<String, Metadata.ClusterCustom> map = new HashMap<>();
            map.put(key, null);
            assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
        }
        {
            final Map<String, Metadata.ProjectCustom> map = new HashMap<>();
            map.put(key, null);
            assertThat(expectThrows(NullPointerException.class, () -> builder.projectCustoms(map)).getMessage(), containsString(key));
        }
    }

    public void testCopyAndUpdate() throws IOException {
        var metadata = Metadata.builder().clusterUUID(UUIDs.base64UUID()).build();
        var newClusterUuid = UUIDs.base64UUID();

        var copy = metadata.copyAndUpdate(builder -> builder.clusterUUID(newClusterUuid));

        assertThat(copy, not(sameInstance(metadata)));
        assertThat(copy.clusterUUID(), equalTo(newClusterUuid));
    }

    public void testBuilderRemoveClusterCustomIf() {
        var custom1 = new TestClusterCustomMetadata();
        var custom2 = new TestClusterCustomMetadata();
        var builder = Metadata.builder();
        builder.putCustom("custom1", custom1);
        builder.putCustom("custom2", custom2);

        builder.removeCustomIf((key, value) -> Objects.equals(key, "custom1"));

        var metadata = builder.build();
        assertThat(metadata.custom("custom1"), nullValue());
        assertThat(metadata.custom("custom2"), sameInstance(custom2));
    }

    public void testBuilderRemoveProjectCustomIf() {
        var custom1 = new TestProjectCustomMetadata();
        var custom2 = new TestProjectCustomMetadata();
        var builder = Metadata.builder();
        builder.putCustom("custom1", custom1);
        builder.putCustom("custom2", custom2);

        builder.removeProjectCustomIf((key, value) -> Objects.equals(key, "custom1"));

        var metadata = builder.build();
        assertThat(metadata.getProject().custom("custom1"), nullValue());
        assertThat(metadata.getProject().custom("custom2"), sameInstance(custom2));
    }

    public void testBuilderRejectsDataStreamThatConflictsWithIndex() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        Metadata.Builder b = Metadata.builder()
            .put(idx, false)
            .put(
                IndexMetadata.builder(dataStreamName)
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .build(),
                false
            )
            .put(newInstance(dataStreamName, List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "index, alias, and data stream names need to be unique, but the following duplicates were found [data "
                    + "stream ["
                    + dataStreamName
                    + "] conflicts with index]"
            )
        );
    }

    public void testBuilderRejectsDataStreamThatConflictsWithAlias() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).putAlias(AliasMetadata.builder(dataStreamName).build()).build();
        Metadata.Builder b = Metadata.builder().put(idx, false).put(newInstance(dataStreamName, List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "index, alias, and data stream names need to be unique, but the following duplicates were found ["
                    + dataStreamName
                    + " (alias of ["
                    + DataStream.getDefaultBackingIndexName(dataStreamName, 1)
                    + "]) conflicts with data stream]"
            )
        );
    }

    public void testBuilderRejectsAliasThatRefersToDataStreamBackingIndex() {
        final String dataStreamName = "my-data-stream";
        final String conflictingName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).putAlias(new AliasMetadata.Builder(conflictingName)).build();
        Metadata.Builder b = Metadata.builder().put(idx, false).put(newInstance(dataStreamName, List.of(idx.getIndex())));

        AssertionError e = expectThrows(AssertionError.class, b::build);
        assertThat(e.getMessage(), containsString("aliases [" + conflictingName + "] cannot refer to backing indices of data streams"));
    }

    public void testBuilderForDataStreamWithRandomlyNumberedBackingIndices() {
        final String dataStreamName = "my-data-stream";
        final List<Index> backingIndices = new ArrayList<>();
        final int numBackingIndices = randomIntBetween(2, 5);
        int lastBackingIndexNum = 0;
        Metadata.Builder b = Metadata.builder();
        for (int k = 1; k <= numBackingIndices; k++) {
            lastBackingIndexNum = randomIntBetween(lastBackingIndexNum + 1, lastBackingIndexNum + 50);
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, lastBackingIndexNum))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            backingIndices.add(im.getIndex());
        }

        b.put(newInstance(dataStreamName, backingIndices, lastBackingIndexNum, null));
        Metadata metadata = b.build();
        assertThat(metadata.getProject().dataStreams().keySet(), containsInAnyOrder(dataStreamName));
        assertThat(metadata.getProject().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
    }

    public void testBuildIndicesLookupForDataStreams() {
        Metadata.Builder b = Metadata.builder();
        int numDataStreams = randomIntBetween(2, 8);
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            addDataStream(name, b);
        }

        Metadata metadata = b.build();
        assertThat(metadata.getProject().dataStreams().size(), equalTo(numDataStreams));
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            IndexAbstraction value = metadata.getProject().getIndicesLookup().get(name);
            assertThat(value, notNullValue());
            DataStream ds = metadata.getProject().dataStreams().get(name);
            assertThat(ds, notNullValue());

            assertThat(value.isHidden(), is(false));
            assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(value.getIndices(), hasSize(ds.getIndices().size()));
            assertThat(value.getWriteIndex().getName(), equalTo(DataStream.getDefaultBackingIndexName(name, ds.getGeneration())));
        }
    }

    public void testBuildIndicesLookupForDataStreamAliases() {
        Metadata.Builder b = Metadata.builder();

        addDataStream("d1", b);
        addDataStream("d2", b);
        addDataStream("d3", b);
        addDataStream("d4", b);

        b.put("a1", "d1", null, null);
        b.put("a1", "d2", null, null);
        b.put("a2", "d3", null, null);
        b.put("a3", "d1", null, null);

        Metadata metadata = b.build();
        assertThat(metadata.getProject().dataStreams(), aMapWithSize(4));
        IndexAbstraction value = metadata.getProject().getIndicesLookup().get("d1");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = metadata.getProject().getIndicesLookup().get("d2");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = metadata.getProject().getIndicesLookup().get("d3");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = metadata.getProject().getIndicesLookup().get("d4");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = metadata.getProject().getIndicesLookup().get("a1");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));

        value = metadata.getProject().getIndicesLookup().get("a2");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));

        value = metadata.getProject().getIndicesLookup().get("a3");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));
    }

    public void testDataStreamAliasValidation() {
        Metadata.Builder b = Metadata.builder();
        addDataStream("my-alias", b);
        b.put("my-alias", "my-alias", null, null);
        var e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = Metadata.builder();
        addDataStream("d1", b);
        addDataStream("my-alias", b);
        b.put("my-alias", "d1", null, null);
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = Metadata.builder();
        b.put(
            IndexMetadata.builder("index1")
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .putAlias(new AliasMetadata.Builder("my-alias"))
        );

        addDataStream("d1", b);
        b.put("my-alias", "d1", null, null);
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (my-alias)"));
    }

    public void testDataStreamAliasValidationRestoreScenario() {
        Metadata.Builder b = Metadata.builder();
        b.dataStreams(
            Map.of("my-alias", createDataStream("my-alias")),
            Map.of("my-alias", new DataStreamAlias("my-alias", List.of("my-alias"), null, null))
        );
        var e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = Metadata.builder();
        b.dataStreams(
            Map.of("d1", createDataStream("d1"), "my-alias", createDataStream("my-alias")),
            Map.of("my-alias", new DataStreamAlias("my-alias", List.of("d1"), null, null))
        );
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = Metadata.builder();
        b.put(
            IndexMetadata.builder("index1")
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .putAlias(new AliasMetadata.Builder("my-alias"))
        );
        b.dataStreams(Map.of("d1", createDataStream("d1")), Map.of("my-alias", new DataStreamAlias("my-alias", List.of("d1"), null, null)));
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (my-alias)"));
    }

    private void addDataStream(String name, Metadata.Builder b) {
        int numBackingIndices = randomIntBetween(1, 4);
        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int j = 1; j <= numBackingIndices; j++) {
            IndexMetadata idx = createBackingIndex(name, j).build();
            indices.add(idx.getIndex());
            b.put(idx, true);
        }
        b.put(newInstance(name, indices));
    }

    private DataStream createDataStream(String name) {
        int numBackingIndices = randomIntBetween(1, 4);
        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int j = 1; j <= numBackingIndices; j++) {
            IndexMetadata idx = createBackingIndex(name, j).build();
            indices.add(idx.getIndex());
        }
        return newInstance(name, indices);
    }

    public void testIndicesLookupRecordsDataStreamForBackingIndices() {
        final int numIndices = randomIntBetween(2, 5);
        final int numBackingIndices = randomIntBetween(2, 5);
        final String dataStreamName = "my-data-stream";
        CreateIndexResult result = createIndices(numIndices, numBackingIndices, dataStreamName);

        SortedMap<String, IndexAbstraction> indicesLookup = result.metadata.getProject().getIndicesLookup();
        assertThat(indicesLookup, aMapWithSize(result.indices.size() + result.backingIndices.size() + 1));
        for (Index index : result.indices) {
            assertThat(indicesLookup, hasKey(index.getName()));
            assertNull(indicesLookup.get(index.getName()).getParentDataStream());
        }
        for (Index index : result.backingIndices) {
            assertThat(indicesLookup, hasKey(index.getName()));
            assertNotNull(indicesLookup.get(index.getName()).getParentDataStream());
            assertThat(indicesLookup.get(index.getName()).getParentDataStream().getName(), equalTo(dataStreamName));
        }
    }

    public void testSerialization() throws IOException {
        final Metadata orig = randomMetadata();
        final BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Metadata fromStreamMeta = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertTrue(Metadata.isGlobalStateEquals(orig, fromStreamMeta));
    }

    public void testMultiProjectSerialization() throws IOException {
        // TODO: this whole suite needs to be updated for multiple projects
        ProjectMetadata project1 = randomProject(ProjectId.fromId("1"), 1);
        ProjectMetadata project2 = randomProject(ProjectId.fromId("2"), randomIntBetween(2, 10));
        Metadata metadata = randomMetadata(List.of(project1, project2));
        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // check it deserializes ok
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        Metadata fromStreamMeta = Metadata.readFrom(new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry));

        // check it matches the original object
        assertThat(fromStreamMeta.projects(), aMapWithSize(2));
        for (var original : List.of(project1, project2)) {
            assertThat(fromStreamMeta.projects(), hasKey(original.id()));
            final ProjectMetadata fromStreamProject = fromStreamMeta.getProject(original.id());
            assertThat("For project " + original.id(), fromStreamProject.indices().size(), equalTo(original.indices().size()));
            assertThat("For project " + original.id(), fromStreamProject.dataStreams().size(), equalTo(original.dataStreams().size()));
            assertThat("For project " + original.id(), fromStreamProject.templates().size(), equalTo(original.templates().size()));
            assertThat("For project " + original.id(), fromStreamProject.templatesV2().size(), equalTo(original.templatesV2().size()));
            original.indices().forEach((name, value) -> {
                assertThat(fromStreamProject.indices(), hasKey(name));
                assertThat(fromStreamProject.index(name), equalTo(value));
            });
            original.dataStreams().forEach((name, value) -> {
                assertThat(fromStreamProject.dataStreams(), hasKey(name));
                assertThat(fromStreamProject.dataStreams().get(name), equalTo(value));
            });
        }
    }

    public void testGetNonExistingProjectThrows() {
        final List<ProjectMetadata> projects = IntStream.range(0, between(1, 3))
            .mapToObj(i -> randomProject(ProjectId.fromId("p_" + i), between(0, 5)))
            .toList();
        final Metadata metadata = randomMetadata(projects);
        expectThrows(IllegalArgumentException.class, () -> metadata.getProject(randomProjectIdOrDefault()));
    }

    public void testValidateDataStreamsNoConflicts() {
        Metadata metadata = createIndices(5, 10, "foo-datastream").metadata;
        // don't expect any exception when validating a system without indices that would conflict with future backing indices
        assertDataStreams(
            metadata.getProject().indices(),
            (DataStreamMetadata) metadata.getProject().customs().get(DataStreamMetadata.TYPE)
        );
    }

    public void testValidateDataStreamsIgnoresIndicesWithoutCounter() {
        String dataStreamName = "foo-datastream";
        Metadata metadata = Metadata.builder(createIndices(10, 10, dataStreamName).metadata)
            .put(
                new IndexMetadata.Builder(dataStreamName + "-index-without-counter").settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .put(
                new IndexMetadata.Builder(dataStreamName + randomAlphaOfLength(10)).settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(1)

            )
            .put(
                new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(1)

            )
            .build();
        // don't expect any exception when validating against non-backing indices that don't conform to the backing indices naming
        // convention
        assertDataStreams(
            metadata.getProject().indices(),
            (DataStreamMetadata) metadata.getProject().customs().get(DataStreamMetadata.TYPE)
        );
    }

    public void testValidateDataStreamsAllowsNamesThatStartsWithPrefix() {
        String dataStreamName = "foo-datastream";
        Metadata metadata = Metadata.builder(createIndices(10, 10, dataStreamName).metadata)
            .put(
                new IndexMetadata.Builder(DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-something-100012").settings(
                    settings(IndexVersion.current())
                ).numberOfShards(1).numberOfReplicas(1)
            )
            .build();
        // don't expect any exception when validating against (potentially backing) indices that can't create conflict because of
        // additional text before number
        assertDataStreams(
            metadata.getProject().indices(),
            (DataStreamMetadata) metadata.getProject().customs().get(DataStreamMetadata.TYPE)
        );
    }

    public void testValidateDataStreamsForNullDataStreamMetadata() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("foo-index").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        try {
            assertDataStreams(metadata.getProject().indices(), DataStreamMetadata.EMPTY);
        } catch (Exception e) {
            fail("did not expect exception when validating a system without any data streams but got " + e.getMessage());
        }
    }

    public void testDataStreamAliases() {
        Metadata.Builder mdBuilder = Metadata.builder();

        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null), is(true));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-us", null, null), is(true));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-au", null, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-au", null, null), is(false));

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );
    }

    public void testDataStreamReferToNonExistingDataStream() {
        Metadata.Builder mdBuilder = Metadata.builder();

        Exception e = expectThrows(IllegalArgumentException.class, () -> mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null));
        assertThat(e.getMessage(), equalTo("alias [logs-postgres] refers to a non existing data stream [logs-postgres-eu]"));

        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));
    }

    public void testDeleteDataStreamShouldUpdateAlias() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeDataStream("logs-postgres-us");
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-au")
        );

        mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeDataStream("logs-postgres-au");
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));

        mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeDataStream("logs-postgres-eu");
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), nullValue());
    }

    public void testDeleteDataStreamAlias() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-us", true), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-au")
        );

        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-au", true), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));

        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-eu", true), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), nullValue());
    }

    public void testDeleteDataStreamAliasMustExists() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        Metadata.Builder mdBuilder2 = Metadata.builder(metadata);
        expectThrows(ResourceNotFoundException.class, () -> mdBuilder2.removeDataStreamAlias("logs-mysql", "logs-postgres-us", true));
        assertThat(mdBuilder2.removeDataStreamAlias("logs-mysql", "logs-postgres-us", false), is(false));
    }

    public void testDataStreamWriteAlias() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null);

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );

        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null), is(true));

        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(),
            equalTo("logs-postgres-replicated")
        );
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );
    }

    public void testDataStreamMultipleWriteAlias() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-foobar"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-barbaz"));
        mdBuilder.put("logs", "logs-foobar", true, null);
        mdBuilder.put("logs", "logs-barbaz", true, null);

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs").getWriteDataStream(), equalTo("logs-barbaz"));
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs").getDataStreams(),
            containsInAnyOrder("logs-foobar", "logs-barbaz")
        );
    }

    public void testDataStreamWriteAliasUnset() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null);

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(),
            equalTo("logs-postgres-replicated")
        );
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );

        mdBuilder = Metadata.builder(metadata);
        // Side check: null value isn't changing anything:
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(false));
        // Unset write flag
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", false, null), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );
    }

    public void testDataStreamWriteAliasChange() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        // change write flag:
        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", false, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(),
            equalTo("logs-postgres-replicated")
        );
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );
    }

    public void testDataStreamWriteRemoveAlias() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        mdBuilder = Metadata.builder(metadata);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-primary", randomBoolean()), is(true));
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );
    }

    public void testDataStreamWriteRemoveDataStream() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        Metadata metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeDataStream("logs-postgres-primary");
        metadata = mdBuilder.build();
        assertThat(metadata.getProject().dataStreams().keySet(), contains("logs-postgres-replicated"));
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(metadata.getProject().dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(
            metadata.getProject().dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-replicated")
        );
    }

    public void testReuseIndicesLookup() {
        String indexName = "my-index";
        String aliasName = "my-alias";
        String dataStreamName = "logs-mysql-prod";
        String dataStreamAliasName = "logs-mysql";
        Metadata previous = Metadata.builder().build();

        // Things that should change indices lookup
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata idx = DataStreamTestHelper.createFirstBackingIndex(dataStreamName).build();
            builder.put(idx, true);
            DataStream dataStream = newInstance(dataStreamName, List.of(idx.getIndex()));
            builder.put(dataStream);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            builder.put(dataStreamAliasName, dataStreamName, false, null);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            builder.put(dataStreamAliasName, dataStreamName, true, null);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            builder.put(
                IndexMetadata.builder(indexName)
                    .settings(settings(IndexVersion.current()))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.putAlias(AliasMetadata.builder(aliasName).build());
            builder.put(imBuilder);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build());
            builder.put(imBuilder);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            Settings.Builder sBuilder = Settings.builder()
                .put(builder.get(indexName).getSettings())
                .put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), true);
            imBuilder.settings(sBuilder.build());
            builder.put(imBuilder);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), not(sameInstance(metadata.getProject().getIndicesLookup())));
            previous = metadata;
        }

        // Things that shouldn't change indices lookup
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.numberOfReplicas(2);
            builder.put(imBuilder);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), sameInstance(metadata.getProject().getIndicesLookup()));
            previous = metadata;
        }
        {
            Metadata.Builder builder = Metadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            Settings.Builder sBuilder = Settings.builder()
                .put(builder.get(indexName).getSettings())
                .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), "val");
            imBuilder.settings(sBuilder.build());
            builder.put(imBuilder);
            Metadata metadata = builder.build();
            assertThat(previous.getProject().getIndicesLookup(), sameInstance(metadata.getProject().getIndicesLookup()));
            previous = metadata;
        }
    }

    public void testAliasedIndices() {
        int numAliases = randomIntBetween(32, 64);
        int numIndicesPerAlias = randomIntBetween(8, 16);

        Metadata.Builder builder = Metadata.builder();
        for (int i = 0; i < numAliases; i++) {
            String aliasName = "alias-" + i;
            for (int j = 0; j < numIndicesPerAlias; j++) {
                AliasMetadata.Builder alias = new AliasMetadata.Builder(aliasName);
                if (j == 0) {
                    alias.writeIndex(true);
                }

                String indexName = aliasName + "-" + j;
                builder.put(
                    IndexMetadata.builder(indexName)
                        .settings(settings(IndexVersion.current()))
                        .creationDate(randomNonNegativeLong())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(alias)
                );
            }
        }

        Metadata metadata = builder.build();
        for (int i = 0; i < numAliases; i++) {
            String aliasName = "alias-" + i;
            Set<Index> result = metadata.getProject().aliasedIndices(aliasName);
            Index[] expected = IntStream.range(0, numIndicesPerAlias)
                .mapToObj(j -> aliasName + "-" + j)
                .map(name -> new Index(name, ClusterState.UNKNOWN_UUID))
                .toArray(Index[]::new);
            assertThat(result, containsInAnyOrder(expected));
        }

        // Add a new alias and index
        builder = Metadata.builder(metadata);
        String newAliasName = "alias-new";
        {
            builder.put(
                IndexMetadata.builder(newAliasName + "-1")
                    .settings(settings(IndexVersion.current()))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(newAliasName).writeIndex(true))
            );
        }
        metadata = builder.build();
        assertThat(metadata.getProject().aliasedIndices(), hasSize(numAliases + 1));
        assertThat(metadata.getProject().aliasedIndices(newAliasName), contains(new Index(newAliasName + "-1", ClusterState.UNKNOWN_UUID)));

        // Remove the new alias/index
        builder = Metadata.builder(metadata);
        {
            builder.remove(newAliasName + "-1");
        }
        metadata = builder.build();
        assertThat(metadata.getProject().aliasedIndices(), hasSize(numAliases));
        assertThat(metadata.getProject().aliasedIndices(newAliasName), empty());

        // Add a new alias that points to existing indices
        builder = Metadata.builder(metadata);
        {
            IndexMetadata.Builder imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-1-0"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-2-1"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-3-2"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);
        }
        metadata = builder.build();
        assertThat(metadata.getProject().aliasedIndices(), hasSize(numAliases + 1));
        assertThat(
            metadata.getProject().aliasedIndices(newAliasName),
            containsInAnyOrder(
                new Index("alias-1-0", ClusterState.UNKNOWN_UUID),
                new Index("alias-2-1", ClusterState.UNKNOWN_UUID),
                new Index("alias-3-2", ClusterState.UNKNOWN_UUID)
            )
        );

        // Remove the new alias that points to existing indices
        builder = Metadata.builder(metadata);
        {
            IndexMetadata.Builder imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-1-0"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-2-1"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(metadata.getProject().index("alias-3-2"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);
        }
        metadata = builder.build();
        assertThat(metadata.getProject().aliasedIndices(), hasSize(numAliases));
        assertThat(metadata.getProject().aliasedIndices(newAliasName), empty());
    }

    public static final String SYSTEM_ALIAS_NAME = "system_alias";

    public void testHiddenAliasValidation() {
        final String hiddenAliasName = "hidden_alias";

        IndexMetadata hidden1 = buildIndexWithAlias("hidden1", hiddenAliasName, true, IndexVersion.current(), false);
        IndexMetadata hidden2 = buildIndexWithAlias("hidden2", hiddenAliasName, true, IndexVersion.current(), false);
        IndexMetadata hidden3 = buildIndexWithAlias("hidden3", hiddenAliasName, true, IndexVersion.current(), false);

        IndexMetadata nonHidden = buildIndexWithAlias("nonhidden1", hiddenAliasName, false, IndexVersion.current(), false);
        IndexMetadata unspecified = buildIndexWithAlias("nonhidden2", hiddenAliasName, null, IndexVersion.current(), false);

        {
            // Should be ok:
            metadataWithIndices(hidden1, hidden2, hidden3);
        }

        {
            // Should be ok:
            if (randomBoolean()) {
                metadataWithIndices(nonHidden, unspecified);
            } else {
                metadataWithIndices(unspecified, nonHidden);
            }
        }

        {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> metadataWithIndices(hidden1, hidden2, hidden3, nonHidden)
            );
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] has is_hidden set to true on indices ["));
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString(hidden1.getIndex().getName()),
                    containsString(hidden2.getIndex().getName()),
                    containsString(hidden3.getIndex().getName())
                )
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "but does not have is_hidden set to true on indices ["
                        + nonHidden.getIndex().getName()
                        + "]; alias must have the same is_hidden setting on all indices"
                )
            );
        }

        {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> metadataWithIndices(hidden1, hidden2, hidden3, unspecified)
            );
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] has is_hidden set to true on indices ["));
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString(hidden1.getIndex().getName()),
                    containsString(hidden2.getIndex().getName()),
                    containsString(hidden3.getIndex().getName())
                )
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "but does not have is_hidden set to true on indices ["
                        + unspecified.getIndex().getName()
                        + "]; alias must have the same is_hidden setting on all indices"
                )
            );
        }

        {
            final IndexMetadata hiddenIndex = randomFrom(hidden1, hidden2, hidden3);
            IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    metadataWithIndices(nonHidden, unspecified, hiddenIndex);
                } else {
                    metadataWithIndices(unspecified, nonHidden, hiddenIndex);
                }
            });
            assertThat(
                exception.getMessage(),
                containsString(
                    "alias ["
                        + hiddenAliasName
                        + "] has is_hidden set to true on "
                        + "indices ["
                        + hiddenIndex.getIndex().getName()
                        + "] but does not have is_hidden set to true on indices ["
                )
            );
            assertThat(
                exception.getMessage(),
                allOf(containsString(unspecified.getIndex().getName()), containsString(nonHidden.getIndex().getName()))
            );
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices ["));
        }
    }

    public void testSystemAliasValidationMixedVersionSystemAndRegularFails() {
        final IndexVersion random7xVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_7_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, IndexVersion.current(), false);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> metadataWithIndices(currentVersionSystem, oldVersionSystem, regularIndex)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "alias ["
                    + SYSTEM_ALIAS_NAME
                    + "] refers to both system indices ["
                    + currentVersionSystem.getIndex().getName()
                    + "] and non-system indices: ["
                    + regularIndex.getIndex().getName()
                    + "], but aliases must refer to either system or non-system indices, not both"
            )
        );
    }

    public void testSystemAliasValidationNewSystemAndRegularFails() {
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, IndexVersion.current(), false);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> metadataWithIndices(currentVersionSystem, regularIndex)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "alias ["
                    + SYSTEM_ALIAS_NAME
                    + "] refers to both system indices ["
                    + currentVersionSystem.getIndex().getName()
                    + "] and non-system indices: ["
                    + regularIndex.getIndex().getName()
                    + "], but aliases must refer to either system or non-system indices, not both"
            )
        );
    }

    public void testSystemAliasOldSystemAndNewRegular() {
        final IndexVersion random7xVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_7_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
        );
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, IndexVersion.current(), false);

        // Should be ok:
        metadataWithIndices(oldVersionSystem, regularIndex);
    }

    public void testSystemIndexValidationAllRegular() {
        final IndexVersion random7xVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_7_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok
        metadataWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
    }

    public void testSystemAliasValidationAllSystemSomeOld() {
        final IndexVersion random7xVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_7_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok:
        metadataWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
    }

    public void testSystemAliasValidationAll8x() {
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);

        // Should be ok
        metadataWithIndices(currentVersionSystem, currentVersionSystem2);
    }

    private void metadataWithIndices(IndexMetadata... indices) {
        Metadata.Builder builder = Metadata.builder();
        for (var cursor : indices) {
            builder.put(cursor, false);
        }
        builder.build();
    }

    private IndexMetadata buildIndexWithAlias(
        String indexName,
        String aliasName,
        @Nullable Boolean aliasIsHidden,
        IndexVersion indexCreationVersion,
        boolean isSystem
    ) {
        final AliasMetadata.Builder aliasMetadata = new AliasMetadata.Builder(aliasName);
        if (aliasIsHidden != null || randomBoolean()) {
            aliasMetadata.isHidden(aliasIsHidden);
        }
        return new IndexMetadata.Builder(indexName).settings(settings(indexCreationVersion))
            .system(isSystem)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasMetadata)
            .build();
    }

    public void testMappingDuplication() {
        final Set<String> randomMappingDefinitions;
        {
            int numEntries = randomIntBetween(4, 8);
            randomMappingDefinitions = Sets.newHashSetWithExpectedSize(numEntries);
            for (int i = 0; i < numEntries; i++) {
                Map<String, Object> mapping = RandomAliasActionsGenerator.randomMap(2);
                String mappingAsString = Strings.toString((builder, params) -> builder.mapContents(mapping));
                randomMappingDefinitions.add(mappingAsString);
            }
        }

        Metadata metadata;
        int numIndices = randomIntBetween(16, 32);
        {
            String[] definitions = randomMappingDefinitions.toArray(String[]::new);
            Metadata.Builder mb = new Metadata.Builder();
            for (int i = 0; i < numIndices; i++) {
                IndexMetadata.Builder indexBuilder = IndexMetadata.builder("index-" + i)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(definitions[i % randomMappingDefinitions.size()])
                    .numberOfShards(1)
                    .numberOfReplicas(0);
                if (randomBoolean()) {
                    mb.put(indexBuilder);
                } else {
                    mb.put(indexBuilder.build(), true);
                }
            }
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(
            metadata.getProject().indices().values().stream().map(IndexMetadata::mapping).collect(Collectors.toSet()),
            hasSize(metadata.getProject().getMappingsByHash().size())
        );

        // Add a new index with a new index with known mapping:
        MappingMetadata mapping = metadata.getProject().indices().get("index-" + randomInt(numIndices - 1)).mapping();
        MappingMetadata entry = metadata.getProject().getMappingsByHash().get(mapping.getSha256());
        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.put(
                IndexMetadata.builder("index-" + numIndices)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(mapping)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(metadata.getProject().getMappingsByHash().get(mapping.getSha256()), equalTo(entry));

        // Remove index and ensure mapping cache stays the same
        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.remove("index-" + numIndices);
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(metadata.getProject().getMappingsByHash().get(mapping.getSha256()), equalTo(entry));

        // Update a mapping of an index:
        IndexMetadata luckyIndex = metadata.getProject().index("index-" + randomInt(numIndices - 1));
        entry = metadata.getProject().getMappingsByHash().get(luckyIndex.mapping().getSha256());
        MappingMetadata updatedMapping = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of("mapping", "updated"));
        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.put(IndexMetadata.builder(luckyIndex).putMapping(updatedMapping));
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size() + 1));
        assertThat(metadata.getProject().getMappingsByHash().get(luckyIndex.mapping().getSha256()), equalTo(entry));
        assertThat(metadata.getProject().getMappingsByHash().get(updatedMapping.getSha256()), equalTo(updatedMapping));

        // Remove the index with updated mapping
        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.remove(luckyIndex.getIndex().getName());
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(metadata.getProject().getMappingsByHash().get(updatedMapping.getSha256()), nullValue());

        // Add an index with new mapping and then later remove it:
        MappingMetadata newMapping = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of("new", "mapping"));
        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.put(
                IndexMetadata.builder("index-" + numIndices)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(newMapping)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size() + 1));
        assertThat(metadata.getProject().getMappingsByHash().get(newMapping.getSha256()), equalTo(newMapping));

        {
            Metadata.Builder mb = new Metadata.Builder(metadata);
            mb.remove("index-" + numIndices);
            metadata = mb.build();
        }
        assertThat(metadata.getProject().getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(metadata.getProject().getMappingsByHash().get(newMapping.getSha256()), nullValue());
    }

    public void testWithLifecycleState() {
        String indexName = "my-index";
        String indexUUID = randomAlphaOfLength(10);
        Metadata metadata1 = Metadata.builder(randomMetadata())
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        IndexMetadata index1 = metadata1.getProject().index(indexName);
        assertThat(metadata1.getProject().getIndicesLookup(), notNullValue());
        assertThat(index1.getLifecycleExecutionState(), sameInstance(LifecycleExecutionState.EMPTY_STATE));

        LifecycleExecutionState state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").build();
        Metadata metadata2 = metadata1.withLifecycleState(index1.getIndex(), state);
        IndexMetadata index2 = metadata2.getProject().index(indexName);

        // the indices lookups are the same object
        assertThat(metadata2.getProject().getIndicesLookup(), sameInstance(metadata1.getProject().getIndicesLookup()));

        // the lifecycle state and version were changed
        assertThat(index2.getLifecycleExecutionState().asMap(), is(state.asMap()));
        assertThat(index2.getVersion(), is(index1.getVersion() + 1));

        // but those are the only differences between the two
        IndexMetadata.Builder builder = IndexMetadata.builder(index2);
        builder.version(builder.version() - 1);
        builder.removeCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
        assertThat(index1, equalTo(builder.build()));

        // withLifecycleState returns the same reference if nothing changed
        Metadata metadata3 = metadata2.withLifecycleState(index2.getIndex(), state);
        assertThat(metadata3, sameInstance(metadata2));

        // withLifecycleState rejects a nonsense Index
        String randomUUID = randomValueOtherThan(indexUUID, () -> randomAlphaOfLength(10));
        expectThrows(IndexNotFoundException.class, () -> metadata1.withLifecycleState(new Index(indexName, randomUUID), state));
    }

    public void testEmptyDiffReturnsSameInstance() throws IOException {
        final Metadata instance = randomMetadata();
        Diff<Metadata> diff = instance.diff(instance);
        assertSame(instance, diff.apply(instance));
        final BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);
        final Diff<Metadata> deserializedDiff = Metadata.readDiffFrom(out.bytes().streamInput());
        assertSame(instance, deserializedDiff.apply(instance));
    }

    public void testMultiProjectXContent() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final List<ProjectMetadata> projects = randomList(1, 5, () -> randomProject(randomUniqueProjectId(), randomIntBetween(1, 3)))
            .stream()
            .map(
                project -> ProjectMetadata.builder(project)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            List.of(
                                new RepositoryMetadata(
                                    "backup",
                                    "uuid-" + project.id().id(),
                                    "fs",
                                    Settings.builder().put("location", project.id().id()).build()
                                )
                            )
                        )
                    )
                    .build()
            )
            .toList();

        final Metadata originalMeta = Metadata.builder(randomMetadata(projects))
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId + 1,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId + 1,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            )
            .build();

        ToXContent.Params p = new ToXContent.MapParams(
            Map.of("multi-project", "true", Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)
        );

        final BytesReference bytes = toXContentBytes(originalMeta, p);

        // XContent with multi-project=true has separate cluster and project tasks
        final var objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, bytes);
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks"), notNullValue());
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.last_allocation_id"), equalTo(lastAllocationId + 1));
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.tasks"), hasSize(1));
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks.tasks.0.id"), equalTo(HealthNode.TASK_NAME));
        assertThat(objectPath.evaluate("meta-data.projects"), hasSize(projects.size()));
        assertThat(IntStream.range(0, projects.size()).mapToObj(i -> {
            try {
                return (String) objectPath.evaluate("meta-data.projects." + i + ".id");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toUnmodifiableSet()),
            equalTo(projects.stream().map(pp -> pp.id().id()).collect(Collectors.toUnmodifiableSet()))
        );

        Metadata fromXContentMeta = fromJsonXContentStringWithPersistentTasks(bytes.utf8ToString());
        assertThat(fromXContentMeta.projects().keySet(), equalTo(originalMeta.projects().keySet()));
        for (var project : fromXContentMeta.projects().values()) {
            assertThat(
                RepositoriesMetadata.get(project).repositories(),
                equalTo(
                    List.of(
                        new RepositoryMetadata(
                            "backup",
                            "uuid-" + project.id().id(),
                            "fs",
                            Settings.builder().put("location", project.id().id()).build()
                        )
                    )
                )
            );
        }
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(fromXContentMeta);
        assertThat(clusterTasks.getLastAllocationId(), equalTo(lastAllocationId + 1));
        assertThat(clusterTasks.taskMap().keySet(), equalTo(Set.of(HealthNode.TASK_NAME)));
    }

    public void testDefaultProjectXContentWithPersistentTasks() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final var originalMeta = Metadata.builder()
            .clusterUUID(randomUUID())
            .clusterUUIDCommitted(true)
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId + 1,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId + 1,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            )
            .build();

        // For single project metadata, XContent output should combine the cluster and project tasks
        final ToXContent.Params p = new ToXContent.MapParams(
            Map.ofEntries(Map.entry("multi-project", "false"), Map.entry(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_SNAPSHOT))
        );
        final BytesReference bytes = toXContentBytes(originalMeta, p);

        final var objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, bytes);
        // No cluster_persistent_tasks for single project output, it is combined with persistent_tasks
        assertThat(objectPath.evaluate("meta-data.cluster_persistent_tasks"), nullValue());
        // The combined lastAllocationId is the max between cluster and project tasks
        assertThat(objectPath.evaluate("meta-data.persistent_tasks.last_allocation_id"), equalTo(lastAllocationId + 1));
        assertThat(objectPath.evaluate("meta-data.persistent_tasks.tasks"), hasSize(1));

        // Deserialize from the XContent should separate cluster and project tasks
        final Metadata fromXContentMeta = fromJsonXContentStringWithPersistentTasks(bytes.utf8ToString());
        assertThat(fromXContentMeta.projects().keySet(), equalTo(Set.of(ProjectId.DEFAULT)));
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(fromXContentMeta);
        assertThat(clusterTasks, notNullValue());
        assertThat(clusterTasks.getLastAllocationId(), equalTo(lastAllocationId + 1));
        assertThat(
            clusterTasks.tasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getId).toList(),
            contains(HealthNode.TASK_NAME)
        );
    }

    public void testSingleNonDefaultProjectXContent() throws IOException {
        final long lastAllocationId = randomNonNegativeLong();
        final var indexVersion = IndexVersion.current();
        // When ClusterStateAction acts in a project scope, it returns cluster state metadata that has a single project that may
        // not have the default project-id. We need to be able to convert this to XContent in the Rest response
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.fromId("c8af967d644b3219"))
            .put(IndexMetadata.builder("index-1").settings(indexSettings(indexVersion, 1, 1)).build(), false)
            .put(IndexMetadata.builder("index-2").settings(indexSettings(indexVersion, 2, 2)).build(), false)
            .build();
        final Metadata metadata = Metadata.builder()
            .clusterUUID("afSSOgAAQAC8BuQTAAAAAA")
            .clusterUUIDCommitted(true)
            .put(project)
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                new ClusterPersistentTasksCustomMetadata(
                    lastAllocationId,
                    Map.of(
                        HealthNode.TASK_NAME,
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            HealthNode.TASK_NAME,
                            HealthNode.TASK_NAME,
                            HealthNodeTaskParams.INSTANCE,
                            lastAllocationId,
                            PersistentTasks.INITIAL_ASSIGNMENT
                        )
                    )
                )
            )
            .build();
        final ToXContent.Params p = new ToXContent.MapParams(
            Map.ofEntries(Map.entry("multi-project", "false"), Map.entry(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_API))
        );
        var expected = Strings.format("""
            {
              "metadata": {
                "cluster_uuid": "afSSOgAAQAC8BuQTAAAAAA",
                "cluster_uuid_committed":true,
                "cluster_coordination": {
                  "term":0,
                  "last_committed_config":[],
                  "last_accepted_config":[],
                  "voting_config_exclusions": []
                },
                "templates": {},
                "indices": {
                  "index-1": {
                    "version": 1,
                    "mapping_version": 1,
                    "settings_version": 1,
                    "aliases_version": 1,
                    "routing_num_shards": 1,
                    "state": "open",
                    "settings": {
                      "index": {
                        "number_of_shards": "1",
                        "number_of_replicas": "1",
                        "version": {
                          "created": "%s"
                        }
                      }
                    },
                    "mappings": {},
                    "aliases": [],
                    "primary_terms": {
                      "0": 0
                    },
                    "in_sync_allocations": {
                      "0": []
                    },
                    "rollover_info": {},
                    "mappings_updated_version": %s,
                    "system": false,
                    "timestamp_range": {
                      "shards": []
                    },
                    "event_ingested_range": {
                      "shards": []
                    }
                  },
                  "index-2": {
                    "version": 1,
                    "mapping_version": 1,
                    "settings_version": 1,
                    "aliases_version": 1,
                    "routing_num_shards": 2,
                    "state": "open",
                    "settings": {
                      "index": {
                        "number_of_shards": "2",
                        "number_of_replicas": "2",
                        "version": {
                          "created": "%s"
                        }
                      }
                    },
                    "mappings": {},
                    "aliases": [],
                    "primary_terms": {
                      "0": 0,
                      "1": 0
                    },
                    "in_sync_allocations": {
                      "1": [],
                      "0": []
                    },
                    "rollover_info": {},
                    "mappings_updated_version": %s,
                    "system": false,
                    "timestamp_range": {
                      "shards": []
                    },
                    "event_ingested_range": {
                      "shards": []
                    }
                  }
                },
                "index-graveyard": {
                  "tombstones": []
                },
                "reserved_state": {},
                "persistent_tasks": {
                  "last_allocation_id": %s,
                  "tasks": [
                    {
                      "id": "health-node",
                      "task": { "health-node": {"params":{}} },
                      "assignment": {
                        "explanation": "waiting for initial assignment",
                        "executor_node": null
                      },
                      "allocation_id": %s
                    }
                  ]
                }
              }
            }
            """, indexVersion, indexVersion, indexVersion, indexVersion, lastAllocationId, lastAllocationId);
        assertToXContentEquivalent(new BytesArray(expected), toXContentBytes(metadata, p), XContentType.JSON);
    }

    private static BytesReference toXContentBytes(Metadata metadata, ToXContent.Params params) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(metadata).toXContent(builder, params);
        builder.endObject();
        return BytesReference.bytes(builder);
    }

    public void testChunkedToXContent() {
        AbstractChunkedSerializingTestCase.assertChunkCount(randomMetadata(randomInt(10)), MetadataTests::expectedChunkCount);
    }

    private static int expectedChunkCount(Metadata metadata) {
        return expectedChunkCount(ToXContent.EMPTY_PARAMS, metadata);
    }

    public static int expectedChunkCount(ToXContent.Params params, Metadata metadata) {
        final var context = Metadata.XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));

        // 2 chunks at the beginning
        long chunkCount = 2;
        // 1 optional chunk for persistent settings
        if (context != Metadata.XContentContext.API && metadata.persistentSettings().isEmpty() == false) {
            chunkCount += 1;
        }

        for (Metadata.ClusterCustom custom : metadata.customs().values()) {
            chunkCount += 2;
            if (custom instanceof DesiredNodesMetadata) {
                chunkCount += checkChunkSize(custom, params, 1);
            } else if (custom instanceof NodesShutdownMetadata nodesShutdownMetadata) {
                chunkCount += checkChunkSize(custom, params, 2 + nodesShutdownMetadata.getAll().size());
            } else {
                // could be anything, we have to just try it
                chunkCount += count(custom.toXContentChunked(params));
            }
        }
        if (context != Metadata.XContentContext.API || params.paramAsBoolean("multi-project", false)) {
            chunkCount += 2; // start/end "projects":[]
            chunkCount += 3L * metadata.projects().size(); // open/close + id-field
        }

        chunkCount += metadata.projects()
            .values()
            .stream()
            .mapToInt(project -> ProjectMetadataTests.expectedChunkCount(params, project))
            .sum();

        int reservedStateSize = metadata.reservedStateMetadata().size();
        if (params.paramAsBoolean("multi-project", false) == false) {
            // only one project if not multi-project, add its reserved state to the cluster's collection
            reservedStateSize += metadata.projects().values().iterator().next().reservedStateMetadata().size();
        }

        // 2 chunks for wrapping reserved state + 1 chunk for each item
        chunkCount += 2 + reservedStateSize;
        // 1 chunk to close metadata
        chunkCount += 1;

        return Math.toIntExact(chunkCount);
    }

    protected static long count(Iterator<?> it) {
        long count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        return count;
    }

    protected static long checkChunkSize(ChunkedToXContent custom, ToXContent.Params params, long expectedSize) {
        long actualSize = count(custom.toXContentChunked(params));
        assertThat(actualSize, equalTo(expectedSize));
        return actualSize;
    }

    /**
     * With this test we ensure that we consider whether a new field added to Metadata should be checked
     * in Metadata.isGlobalStateEquals. We force the instance fields to be either in the checked list
     * or in the excluded list.
     * <p>
     * This prevents from accidentally forgetting that a new field should be checked in isGlobalStateEquals.
     */
    @SuppressForbidden(reason = "need access to all fields, they are mostly private")
    public void testEnsureMetadataFieldCheckedForGlobalStateChanges() {
        Set<String> checkedForGlobalStateChanges = Set.of(
            "coordinationMetadata",
            "persistentSettings",
            "hashesOfConsistentSettings",
            "templates",
            "clusterUUID",
            "clusterUUIDCommitted",
            "customs",
            "reservedStateMetadata"
        );
        Set<String> excludedFromGlobalStateCheck = Set.of(
            "projectMetadata",
            "version",
            "transientSettings",
            "settings",
            "indices",
            "aliasedIndices",
            "totalNumberOfShards",
            "totalOpenIndexShards",
            "allIndices",
            "visibleIndices",
            "allOpenIndices",
            "visibleOpenIndices",
            "allClosedIndices",
            "visibleClosedIndices",
            "indicesLookup",
            "mappingsByHash",
            "oldestIndexVersion",
            "projectLookup"
        );

        var diff = new HashSet<>(checkedForGlobalStateChanges);
        diff.removeAll(excludedFromGlobalStateCheck);

        // sanity check that the two field sets are mutually exclusive
        assertEquals(checkedForGlobalStateChanges, diff);

        // any declared non-static field in metadata must be either in the list of fields
        // we check for global state changes, or in the fields excluded from the global state check.
        var unclassifiedFields = Arrays.stream(Metadata.class.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) == false)
            .map(Field::getName)
            .filter(n -> (checkedForGlobalStateChanges.contains(n) || excludedFromGlobalStateCheck.contains(n)) == false)
            .collect(Collectors.toSet());
        assertThat(unclassifiedFields, empty());
    }

    public void testRetrieveIndexModeFromTemplateTsdb() throws IOException {
        // tsdb:
        var tsdbTemplate = new Template(Settings.builder().put("index.mode", "time_series").build(), new CompressedXContent("{}"), null);
        // Settings in component template:
        {
            var componentTemplate = new ComponentTemplate(tsdbTemplate, null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.TIME_SERIES));
        }
        // Settings in composable index template:
        {
            var componentTemplate = new ComponentTemplate(new Template(null, null, null), null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .template(tsdbTemplate)
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.TIME_SERIES));
        }
    }

    public void testRetrieveIndexModeFromTemplateLogsdb() throws IOException {
        // logsdb:
        var logsdbTemplate = new Template(Settings.builder().put("index.mode", "logsdb").build(), new CompressedXContent("{}"), null);
        // Settings in component template:
        {
            var componentTemplate = new ComponentTemplate(logsdbTemplate, null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.LOGSDB));
        }
        // Settings in composable index template:
        {
            var componentTemplate = new ComponentTemplate(new Template(null, null, null), null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .template(logsdbTemplate)
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.LOGSDB));
        }
    }

    public void testRetrieveIndexModeFromTemplateEmpty() throws IOException {
        // no index mode:
        var emptyTemplate = new Template(Settings.EMPTY, new CompressedXContent("{}"), null);
        // Settings in component template:
        {
            var componentTemplate = new ComponentTemplate(emptyTemplate, null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), nullValue());
        }
        // Settings in composable index template:
        {
            var componentTemplate = new ComponentTemplate(new Template(null, null, null), null, null);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-*"))
                .template(emptyTemplate)
                .componentTemplates(List.of("component_template_1"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            Metadata m = Metadata.builder().put("component_template_1", componentTemplate).put("index_template_1", indexTemplate).build();
            assertThat(m.getProject().retrieveIndexModeFromTemplate(indexTemplate), nullValue());
        }
    }

    public void testGetSingleProjectWithCustom() {
        var type = IngestMetadata.TYPE;
        {
            Metadata metadata = Metadata.builder().build();
            assertNull(metadata.getSingleProjectCustom(type));
            assertNull(metadata.getSingleProjectWithCustom(type));
        }
        {
            Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
            assertNull(metadata.getSingleProjectCustom(type));
            assertNull(metadata.getSingleProjectWithCustom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertEquals(ingestMetadata, metadata.getSingleProjectCustom(type));
            assertEquals(ingestMetadata, metadata.getSingleProjectWithCustom(type).custom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()))
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertEquals(ingestMetadata, metadata.getSingleProjectCustom(type));
            assertEquals(ingestMetadata, metadata.getSingleProjectWithCustom(type).custom(type));
        }
        {
            var ingestMetadata = new IngestMetadata(Map.of());
            Metadata metadata = Metadata.builder()
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, new IngestMetadata(Map.of())))
                .put(ProjectMetadata.builder(randomUniqueProjectId()).putCustom(type, ingestMetadata))
                .build();
            assertThrows(UnsupportedOperationException.class, () -> metadata.getSingleProjectCustom(type));
            assertThrows(UnsupportedOperationException.class, () -> metadata.getSingleProjectWithCustom(type));
        }
    }

    public void testProjectLookupWithSingleProject() {
        final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

        final int numberOfIndices = randomIntBetween(2, 20);
        final List<IndexMetadata> indices = new ArrayList<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final String uuid = randomUUID();
            final IndexMetadata indexMetadata = IndexMetadata.builder(org.elasticsearch.core.Strings.format("index-%02d", i))
                .settings(
                    indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                )
                .build();
            indices.add(indexMetadata);
            projectBuilder.put(indexMetadata, false);
        }

        final Metadata.ProjectLookup lookup = Metadata.builder().put(projectBuilder).build().getProjectLookup();
        assertThat(lookup, Matchers.instanceOf(Metadata.SingleProjectLookup.class));
        indices.forEach(im -> {
            final Index index = im.getIndex();
            assertThat(lookup.project(index).map(ProjectMetadata::id), isPresentWith(projectId));

            Index alt1 = new Index(index.getName(), randomValueOtherThan(im.getIndexUUID(), ESTestCase::randomUUID));
            assertThat(lookup.project(alt1), isEmpty());

            Index alt2 = new Index(randomAlphaOfLength(8), im.getIndexUUID());
            assertThat(lookup.project(alt2), isEmpty());
        });
    }

    public void testProjectLookupWithMultipleProjects() {
        final int numberOfProjects = randomIntBetween(2, 8);
        final Metadata.Builder metadataBuilder = Metadata.builder();
        final Map<ProjectId, List<IndexMetadata>> indices = Maps.newMapWithExpectedSize(numberOfProjects);
        for (int p = 1; p <= numberOfProjects; p++) {
            final ProjectId projectId = ProjectId.fromId(org.elasticsearch.core.Strings.format("proj_%02d", p));
            final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

            final int numberOfIndices = randomIntBetween(p, 10);
            indices.put(projectId, new ArrayList<>(numberOfIndices));
            for (int i = 0; i < numberOfIndices; i++) {
                final String uuid = randomUUID();
                final IndexMetadata indexMetadata = IndexMetadata.builder(org.elasticsearch.core.Strings.format("index-%02d", i))
                    .settings(
                        indexSettings(1, 0).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    )
                    .build();
                indices.get(projectId).add(indexMetadata);
                projectBuilder.put(indexMetadata, false);
            }
            metadataBuilder.put(projectBuilder);
        }

        final Metadata.ProjectLookup lookup = metadataBuilder.build().getProjectLookup();
        assertThat(lookup, Matchers.instanceOf(Metadata.MultiProjectLookup.class));

        indices.forEach((project, ix) -> ix.forEach(im -> {
            final Index index = im.getIndex();
            assertThat(lookup.project(index).map(ProjectMetadata::id), isPresentWith(project));

            Index alt1 = new Index(index.getName(), randomValueOtherThan(im.getIndexUUID(), ESTestCase::randomUUID));
            assertThat(lookup.project(alt1), isEmpty());

            Index alt2 = new Index(randomAlphaOfLength(8), im.getIndexUUID());
            assertThat(lookup.project(alt2), isEmpty());
        }));
    }

    public void testOldestIndexVersionAllProjects() {
        final int numProjects = between(1, 5);
        final List<IndexVersion> indexVersions = randomList(
            numProjects,
            numProjects,
            () -> IndexVersionUtils.randomCompatibleVersion(random())
        );

        final Map<ProjectId, ProjectMetadata> projectMetadataMap = new HashMap<>();
        for (int i = 0; i < numProjects; i++) {
            final var projectId = i == 0 ? Metadata.DEFAULT_PROJECT_ID : randomUniqueProjectId();
            projectMetadataMap.put(
                projectId,
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(randomIdentifier()).settings(indexSettings(indexVersions.get(i), 1, 1)))
                    .build()
            );
        }
        final Metadata metadata = Metadata.builder(Metadata.EMPTY_METADATA).projectMetadata(projectMetadataMap).build();

        assertThat(metadata.oldestIndexVersionAllProjects(), equalTo(indexVersions.stream().min(Comparator.naturalOrder()).orElseThrow()));
    }

    public static Metadata randomMetadata() {
        return randomMetadata(1);
    }

    public static Metadata randomMetadata(int numDataStreams) {
        final ProjectMetadata project = randomProject(Metadata.DEFAULT_PROJECT_ID, numDataStreams);
        return randomMetadata(List.of(project));
    }

    public static Metadata randomMetadata(Collection<ProjectMetadata> projects) {
        final Metadata.Builder md = Metadata.builder()
            .persistentSettings(Settings.builder().put("setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .transientSettings(Settings.builder().put("other_setting" + randomAlphaOfLength(3), randomAlphaOfLength(4)).build())
            .clusterUUID("uuid" + randomAlphaOfLength(3))
            .clusterUUIDCommitted(randomBoolean())
            .version(randomNonNegativeLong());
        for (var project : projects) {
            md.put(project);
        }
        return md.build();
    }

    public static ProjectMetadata randomProject(ProjectId id, int numDataStreams) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(id)
            .put(buildIndexMetadata("index", "alias", randomBoolean() ? null : randomBoolean()).build(), randomBoolean())
            .put(
                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                    .build()
            )
            .indexGraveyard(IndexGraveyardTests.createRandom())
            .put("component_template_" + randomAlphaOfLength(3), ComponentTemplateTests.randomInstance())
            .put("index_template_v2_" + randomAlphaOfLength(3), ComposableIndexTemplateTests.randomInstance());

        for (int k = 0; k < numDataStreams; k++) {
            DataStream randomDataStream = DataStreamTestHelper.randomInstance();
            for (Index index : randomDataStream.getIndices()) {
                project.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
            }
            project.put(randomDataStream);
        }
        return project.build();
    }

    private static CreateIndexResult createIndices(int numIndices, int numBackingIndices, String dataStreamName) {
        // create some indices that do not back a data stream
        final List<Index> indices = new ArrayList<>();
        int lastIndexNum = randomIntBetween(9, 50);
        Metadata.Builder b = Metadata.builder();
        for (int k = 1; k <= numIndices; k++) {
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("index", lastIndexNum))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            indices.add(im.getIndex());
            lastIndexNum = randomIntBetween(lastIndexNum + 1, lastIndexNum + 50);
        }

        // create some backing indices for a data stream
        final List<Index> backingIndices = new ArrayList<>();
        int lastBackingIndexNum = 0;
        for (int k = 1; k <= numBackingIndices; k++) {
            lastBackingIndexNum = randomIntBetween(lastBackingIndexNum + 1, lastBackingIndexNum + 50);
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, lastBackingIndexNum))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            backingIndices.add(im.getIndex());
        }
        b.put(newInstance(dataStreamName, backingIndices, lastBackingIndexNum, null));
        return new CreateIndexResult(indices, backingIndices, b.build());
    }

    private static class CreateIndexResult {
        final List<Index> indices;
        final List<Index> backingIndices;
        final Metadata metadata;

        CreateIndexResult(List<Index> indices, List<Index> backingIndices, Metadata metadata) {
            this.indices = indices;
            this.backingIndices = backingIndices;
            this.metadata = metadata;
        }
    }

    private abstract static class AbstractCustomMetadata<C extends Metadata.MetadataCustom<C>> implements Metadata.MetadataCustom<C> {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        @Override
        public Diff<C> diff(C previousState) {
            return null;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return null;
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    private static class TestClusterCustomMetadata extends AbstractCustomMetadata<Metadata.ClusterCustom>
        implements
            Metadata.ClusterCustom {}

    private static class TestProjectCustomMetadata extends AbstractCustomMetadata<Metadata.ProjectCustom>
        implements
            Metadata.ProjectCustom {}
}
