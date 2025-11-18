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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.alias.RandomAliasActionsGenerator;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import static org.elasticsearch.cluster.metadata.MetadataTests.checkChunkSize;
import static org.elasticsearch.cluster.metadata.MetadataTests.count;
import static org.elasticsearch.cluster.metadata.ProjectMetadata.Builder.assertDataStreams;
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
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

public class ProjectMetadataTests extends ESTestCase {
    public void testFindAliases() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
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
            Map<String, List<AliasMetadata>> aliases = project.findAliases(request.aliases(), Strings.EMPTY_ARRAY);
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
            Map<String, List<AliasMetadata>> aliases = project.findAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
        }
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "alias*");
            Map<String, List<AliasMetadata>> aliases = project.findAliases(request.aliases(), new String[] { "index", "index2" });
            assertThat(aliases, aMapWithSize(2));
            List<AliasMetadata> indexAliasMetadataList = aliases.get("index");
            assertThat(indexAliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
            List<AliasMetadata> index2AliasMetadataList = aliases.get("index2");
            assertThat(index2AliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias2", "alias3")));
        }
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "alias1");
            Map<String, List<AliasMetadata>> aliases = project.findAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1")));
        }
        {
            Map<String, List<AliasMetadata>> aliases = project.findAllAliases(new String[] { "index" });
            assertThat(aliases, aMapWithSize(1));
            List<AliasMetadata> aliasMetadataList = aliases.get("index");
            assertThat(aliasMetadataList, transformedItemsMatch(AliasMetadata::alias, contains("alias1", "alias2")));
        }
        {
            Map<String, List<AliasMetadata>> aliases = project.findAllAliases(Strings.EMPTY_ARRAY);
            assertThat(aliases, anEmptyMap());
        }
    }

    public void testFindDataStreamAliases() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());

        addDataStream("d1", builder);
        addDataStream("d2", builder);
        addDataStream("d3", builder);
        addDataStream("d4", builder);

        builder.put("alias1", "d1", null, null);
        builder.put("alias2", "d2", null, null);
        builder.put("alias2-part2", "d2", null, null);

        ProjectMetadata project = builder.build();

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
            Map<String, List<DataStreamAlias>> aliases = project.findDataStreamAliases(request.aliases(), Strings.EMPTY_ARRAY);
            assertThat(aliases, anEmptyMap());
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("alias1");
            Map<String, List<DataStreamAlias>> aliases = project.findDataStreamAliases(request.aliases(), new String[] { "index" });
            assertThat(aliases, anEmptyMap());
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("alias1");
            Map<String, List<DataStreamAlias>> aliases = project.findDataStreamAliases(
                request.aliases(),
                new String[] { "index", "d1", "d2" }
            );
            assertEquals(1, aliases.size());
            List<DataStreamAlias> found = aliases.get("d1");
            assertThat(found, transformedItemsMatch(DataStreamAlias::getAlias, contains("alias1")));
        }

        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("ali*");
            Map<String, List<DataStreamAlias>> aliases = project.findDataStreamAliases(request.aliases(), new String[] { "index", "d2" });
            assertEquals(1, aliases.size());
            List<DataStreamAlias> found = aliases.get("d2");
            assertThat(found, transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
        }

        // test exclusion
        {
            GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).aliases("*");
            Map<String, List<DataStreamAlias>> aliases = project.findDataStreamAliases(
                request.aliases(),
                new String[] { "index", "d1", "d2", "d3", "d4" }
            );
            assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
            assertThat(aliases.get("d1"), transformedItemsMatch(DataStreamAlias::getAlias, contains("alias1")));

            request.aliases("*", "-alias1");
            aliases = project.findDataStreamAliases(request.aliases(), new String[] { "index", "d1", "d2", "d3", "d4" });
            assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
            assertNull(aliases.get("d1"));
        }
    }

    public void testDataStreamAliasesByDataStream() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());

        addDataStream("d1", builder);
        addDataStream("d2", builder);
        addDataStream("d3", builder);
        addDataStream("d4", builder);

        builder.put("alias1", "d1", null, null);
        builder.put("alias2", "d2", null, null);
        builder.put("alias2-part2", "d2", null, null);

        ProjectMetadata project = builder.build();

        var aliases = project.dataStreamAliasesByDataStream();

        assertTrue(aliases.containsKey("d1"));
        assertTrue(aliases.containsKey("d2"));
        assertFalse(aliases.containsKey("d3"));
        assertFalse(aliases.containsKey("d4"));

        assertEquals(1, aliases.get("d1").size());
        assertEquals(2, aliases.get("d2").size());

        assertThat(aliases.get("d2"), transformedItemsMatch(DataStreamAlias::getAlias, containsInAnyOrder("alias2", "alias2-part2")));
    }

    public void testFindAliasWithExclusion() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
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
        Map<String, List<AliasMetadata>> aliases = project.findAliases(request.aliases(), new String[] { "index", "index2" });
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
        Map<String, DataStream> dataStreams = result.project.findDataStreams(concreteIndices);
        assertThat(dataStreams, aMapWithSize(numBackingIndices));
        for (Index backingIndex : result.backingIndices) {
            assertThat(dataStreams, hasKey(backingIndex.getName()));
            assertThat(dataStreams.get(backingIndex.getName()).getName(), equalTo(dataStreamName));
        }
    }

    public void testFindAliasWithExclusionAndOverride() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
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
        List<AliasMetadata> aliases = project.findAliases(request.aliases(), new String[] { "index" }).get("index");
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
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
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
            projectBuilder.put(indexBuilder);
        }

        Exception e = expectThrows(IllegalStateException.class, projectBuilder::build);
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
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(buildIndexMetadata(indexA, alias, aWriteIndex))
            .build();

        // when alias points to two indices, but valid
        // one of the following combinations: [(null, null), (null, true), (null, false), (false, false)]
        ProjectMetadata.builder(project).put(buildIndexMetadata(indexB, alias, bWriteIndex)).build();

        // when too many write indices
        Exception exception = expectThrows(IllegalStateException.class, () -> {
            IndexMetadata.Builder metaA = buildIndexMetadata(indexA, alias, true);
            IndexMetadata.Builder metaB = buildIndexMetadata(indexB, alias, true);
            ProjectMetadata.builder(randomProjectIdOrDefault()).put(metaA).put(metaB).build();
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

    private ProjectMetadata.Builder buildMetadataWithHiddenIndexMix(
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
        return ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexAMeta).put(indexBMeta);
    }

    public void testResolveIndexRouting() {
        IndexMetadata.Builder builder = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build())
            .putAlias(AliasMetadata.builder("alias1").routing("1").build())
            .putAlias(AliasMetadata.builder("alias2").routing("1,2").build());
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(builder).build();

        // no alias, no index
        assertNull(project.resolveIndexRouting(null, null));
        assertEquals(project.resolveIndexRouting("0", null), "0");

        // index, no alias
        assertNull(project.resolveIndexRouting(null, "index"));
        assertEquals(project.resolveIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertNull(project.resolveIndexRouting(null, "alias0"));
        assertEquals(project.resolveIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(project.resolveIndexRouting(null, "alias1"), "1");
        Exception ex = expectThrows(IllegalArgumentException.class, () -> project.resolveIndexRouting("0", "alias1"));
        assertThat(
            ex.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation")
        );

        // alias with invalid index routing.
        ex = expectThrows(IllegalArgumentException.class, () -> project.resolveIndexRouting(null, "alias2"));
        assertThat(
            ex.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        ex = expectThrows(IllegalArgumentException.class, () -> project.resolveIndexRouting("1", "alias2"));
        assertThat(
            ex.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        IndexMetadata.Builder builder2 = IndexMetadata.builder("index2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias0").build());
        ProjectMetadata projectTwoIndices = ProjectMetadata.builder(project).put(builder2).build();

        // alias with multiple indices
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> projectTwoIndices.resolveIndexRouting("1", "alias0")
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
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(builder).build();

        // no alias, no index
        assertNull(project.resolveWriteIndexRouting(null, null));
        assertEquals(project.resolveWriteIndexRouting("0", null), "0");

        // index, no alias
        assertNull(project.resolveWriteIndexRouting(null, "index"));
        assertEquals(project.resolveWriteIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertNull(project.resolveWriteIndexRouting(null, "alias0"));
        assertEquals(project.resolveWriteIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(project.resolveWriteIndexRouting(null, "alias1"), "1");
        Exception exception = expectThrows(IllegalArgumentException.class, () -> project.resolveWriteIndexRouting("0", "alias1"));
        assertThat(
            exception.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation")
        );

        // alias with invalid index routing.
        exception = expectThrows(IllegalArgumentException.class, () -> project.resolveWriteIndexRouting(null, "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(IllegalArgumentException.class, () -> project.resolveWriteIndexRouting("1", "alias2"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );
        exception = expectThrows(IllegalArgumentException.class, () -> project.resolveWriteIndexRouting(randomFrom("1", null), "alias4"));
        assertThat(
            exception.getMessage(),
            is("index/alias [alias4] provided with routing value [1,2] that resolved to several routing values, rejecting operation")
        );

        // alias with no write index
        exception = expectThrows(IllegalArgumentException.class, () -> project.resolveWriteIndexRouting("1", "alias3"));
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
        ProjectMetadata projectTwoIndices = ProjectMetadata.builder(project).put(builder2).build();

        // verify that new write index is used
        assertThat("0", equalTo(projectTwoIndices.resolveWriteIndexRouting("0", "alias1")));
    }

    public void testFindMappings() throws IOException {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(FIND_MAPPINGS_TEST_ITEM))
            .put(IndexMetadata.builder("index2").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(FIND_MAPPINGS_TEST_ITEM))
            .build();

        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = project.findMappings(
                Strings.EMPTY_ARRAY,
                MapperPlugin.NOOP_FIELD_FILTER,
                onNextIndexCalls::incrementAndGet
            );
            assertThat(mappings, anEmptyMap());
            assertThat(onNextIndexCalls.get(), equalTo(0));
        }
        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = project.findMappings(
                new String[] { "index1" },
                MapperPlugin.NOOP_FIELD_FILTER,
                onNextIndexCalls::incrementAndGet
            );
            assertThat(mappings, aMapWithSize(1));
            assertIndexMappingsNotFiltered(mappings, "index1");
            assertThat(onNextIndexCalls.get(), equalTo(1));
        }
        {
            AtomicInteger onNextIndexCalls = new AtomicInteger(0);
            Map<String, MappingMetadata> mappings = project.findMappings(
                new String[] { "index1", "index2" },
                MapperPlugin.NOOP_FIELD_FILTER,
                onNextIndexCalls::incrementAndGet
            );
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

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(originalMappingMetadata))
            .build();

        {
            Map<String, MappingMetadata> mappings = project.findMappings(
                new String[] { "index1" },
                MapperPlugin.NOOP_FIELD_FILTER,
                Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP
            );
            MappingMetadata mappingMetadata = mappings.get("index1");
            assertSame(originalMappingMetadata, mappingMetadata);
        }
        {
            Map<String, MappingMetadata> mappings = project.findMappings(
                new String[] { "index1" },
                index -> field -> randomBoolean(),
                Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP
            );
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

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .put(IndexMetadata.builder("index2").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .put(IndexMetadata.builder("index3").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping))
            .build();

        {
            Map<String, MappingMetadata> mappings = project.findMappings(new String[] { "index1", "index2", "index3" }, index -> {
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
            Map<String, MappingMetadata> mappings = project.findMappings(
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
            Map<String, MappingMetadata> mappings = project.findMappings(
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
        ProjectMetadata project = buildIndicesWithVersions(
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            IndexVersion.fromId(IndexVersion.current().id() + 1)
        ).build();

        assertEquals(IndexVersions.MINIMUM_COMPATIBLE, project.oldestIndexVersion());

        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
        assertEquals(IndexVersion.current(), b.build().oldestIndexVersion());

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

    private ProjectMetadata.Builder buildIndicesWithVersions(IndexVersion... indexVersions) {
        int lastIndexNum = randomIntBetween(9, 50);
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
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

    public void testBuilderRejectsNullCustom() {
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        final String key = randomAlphaOfLength(10);
        {
            final Map<String, Metadata.ProjectCustom> map = new HashMap<>();
            map.put(key, null);
            assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
        }
    }

    public void testCopyAndUpdate() {
        var initialIndexUUID = randomUUID();
        final String indexName = randomAlphaOfLengthBetween(4, 12);
        final ProjectMetadata before = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), initialIndexUUID, 1, 1)))
            .build();

        var alteredIndexUUID = randomUUID();
        assertThat(alteredIndexUUID, not(equalTo(initialIndexUUID)));
        final ProjectMetadata after = before.copyAndUpdate(
            builder -> builder.put(IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), alteredIndexUUID, 1, 1)))
        );

        assertThat(after, not(sameInstance(before)));
        assertThat(after.index(indexName).getIndexUUID(), equalTo(alteredIndexUUID));
    }

    public void testBuilderRemoveCustomIf() {
        var custom1 = new TestProjectCustomMetadata();
        var custom2 = new TestProjectCustomMetadata();
        var builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        builder.putCustom("custom1", custom1);
        builder.putCustom("custom2", custom2);

        builder.removeCustomIf((key, value) -> Objects.equals(key, "custom1"));

        var project = builder.build();
        assertThat(project.custom("custom1"), nullValue());
        assertThat(project.custom("custom2"), sameInstance(custom2));
    }

    public void testBuilderRejectsDataStreamThatConflictsWithIndex() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault())
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
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(idx, false)
            .put(newInstance(dataStreamName, List.of(idx.getIndex())));

        IllegalStateException e = expectThrows(IllegalStateException.class, b::build);
        assertThat(
            e.getMessage(),
            containsString(
                "index, alias, and data stream names need to be unique, but the following duplicates were found ["
                    + dataStreamName
                    + " (alias of ["
                    + idx.getIndex().getName()
                    + "]) conflicts with data stream]"
            )
        );
    }

    public void testBuilderRejectsAliasThatRefersToDataStreamBackingIndex() {
        final String dataStreamName = "my-data-stream";
        final String conflictingName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).putAlias(new AliasMetadata.Builder(conflictingName)).build();
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(idx, false)
            .put(newInstance(dataStreamName, List.of(idx.getIndex())));

        AssertionError e = expectThrows(AssertionError.class, b::build);
        assertThat(e.getMessage(), containsString("aliases [" + conflictingName + "] cannot refer to backing indices of data streams"));
    }

    public void testBuilderForDataStreamWithRandomlyNumberedBackingIndices() {
        final String dataStreamName = "my-data-stream";
        final List<Index> backingIndices = new ArrayList<>();
        final int numBackingIndices = randomIntBetween(2, 5);
        int lastBackingIndexNum = 0;
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        ProjectMetadata project = b.build();
        assertThat(project.dataStreams().keySet(), containsInAnyOrder(dataStreamName));
        assertThat(project.dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
    }

    public void testBuildIndicesLookupForDataStreams() {
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
        int numDataStreams = randomIntBetween(2, 8);
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            addDataStream(name, b);
        }

        ProjectMetadata project = b.build();
        assertThat(project.dataStreams().size(), equalTo(numDataStreams));
        for (int i = 0; i < numDataStreams; i++) {
            String name = "data-stream-" + i;
            IndexAbstraction value = project.getIndicesLookup().get(name);
            assertThat(value, notNullValue());
            DataStream ds = project.dataStreams().get(name);
            assertThat(ds, notNullValue());

            assertThat(value.isHidden(), is(false));
            assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(value.getIndices(), hasSize(ds.getIndices().size()));
            assertThat(value.getWriteIndex().getName(), DataStreamTestHelper.backingIndexEqualTo(name, (int) ds.getGeneration()));
        }
    }

    public void testBuildIndicesLookupForDataStreamAliases() {
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());

        addDataStream("d1", b);
        addDataStream("d2", b);
        addDataStream("d3", b);
        addDataStream("d4", b);

        b.put("a1", "d1", null, null);
        b.put("a1", "d2", null, null);
        b.put("a2", "d3", null, null);
        b.put("a3", "d1", null, null);

        ProjectMetadata project = b.build();
        assertThat(project.dataStreams(), aMapWithSize(4));
        IndexAbstraction value = project.getIndicesLookup().get("d1");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = project.getIndicesLookup().get("d2");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = project.getIndicesLookup().get("d3");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = project.getIndicesLookup().get("d4");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));

        value = project.getIndicesLookup().get("a1");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));

        value = project.getIndicesLookup().get("a2");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));

        value = project.getIndicesLookup().get("a3");
        assertThat(value, notNullValue());
        assertThat(value.getType(), equalTo(IndexAbstraction.Type.ALIAS));
    }

    public void testDataStreamAliasValidation() {
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
        addDataStream("my-alias", b);
        b.put("my-alias", "my-alias", null, null);
        var e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = ProjectMetadata.builder(randomProjectIdOrDefault());
        addDataStream("d1", b);
        addDataStream("my-alias", b);
        b.put("my-alias", "d1", null, null);
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
        b.dataStreams(
            Map.of("my-alias", createDataStream("my-alias")),
            Map.of("my-alias", new DataStreamAlias("my-alias", List.of("my-alias"), null, null))
        );
        var e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = ProjectMetadata.builder(randomProjectIdOrDefault());
        b.dataStreams(
            Map.of("d1", createDataStream("d1"), "my-alias", createDataStream("my-alias")),
            Map.of("my-alias", new DataStreamAlias("my-alias", List.of("d1"), null, null))
        );
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));

        b = ProjectMetadata.builder(randomProjectIdOrDefault());
        b.put(
            IndexMetadata.builder("index1")
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .putAlias(new AliasMetadata.Builder("my-alias"))
        );
        b.dataStreams(Map.of("d1", createDataStream("d1")), Map.of("my-alias", new DataStreamAlias("my-alias", List.of("d1"), null, null)));
        e = expectThrows(IllegalStateException.class, b::build);
        assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (my-alias)"));
    }

    private void addDataStream(String name, ProjectMetadata.Builder b) {
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

        SortedMap<String, IndexAbstraction> indicesLookup = result.project.getIndicesLookup();
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

    public void testValidateDataStreamsNoConflicts() {
        ProjectMetadata project = createIndices(5, 10, "foo-datastream").project;
        // don't expect any exception when validating a system without indices that would conflict with future backing indices
        assertDataStreams(project.indices(), (DataStreamMetadata) project.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsIgnoresIndicesWithoutCounter() {
        String dataStreamName = "foo-datastream";
        ProjectMetadata project = ProjectMetadata.builder(createIndices(10, 10, dataStreamName).project)
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
        assertDataStreams(project.indices(), (DataStreamMetadata) project.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsAllowsNamesThatStartsWithPrefix() {
        String dataStreamName = "foo-datastream";
        ProjectMetadata project = ProjectMetadata.builder(createIndices(10, 10, dataStreamName).project)
            .put(
                new IndexMetadata.Builder(DataStream.BACKING_INDEX_PREFIX + dataStreamName + "-something-100012").settings(
                    settings(IndexVersion.current())
                ).numberOfShards(1).numberOfReplicas(1)
            )
            .build();
        // don't expect any exception when validating against (potentially backing) indices that can't create conflict because of
        // additional text before number
        assertDataStreams(project.indices(), (DataStreamMetadata) project.customs().get(DataStreamMetadata.TYPE));
    }

    public void testValidateDataStreamsForNullDataStreamMetadata() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("foo-index").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        try {
            assertDataStreams(project.indices(), DataStreamMetadata.EMPTY);
        } catch (Exception e) {
            fail("did not expect exception when validating a system without any data streams but got " + e.getMessage());
        }
    }

    public void testDataStreamAliases() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());

        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null), is(true));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-us", null, null), is(true));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-au", null, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-au", null, null), is(false));

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );
    }

    public void testDataStreamReferToNonExistingDataStream() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());

        Exception e = expectThrows(IllegalArgumentException.class, () -> mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null));
        assertThat(e.getMessage(), equalTo("alias [logs-postgres] refers to a non existing data stream [logs-postgres-eu]"));

        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));
    }

    public void testDeleteDataStreamShouldUpdateAlias() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        mdBuilder = ProjectMetadata.builder(project);
        mdBuilder.removeDataStream("logs-postgres-us");
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-au")
        );

        mdBuilder = ProjectMetadata.builder(project);
        mdBuilder.removeDataStream("logs-postgres-au");
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));

        mdBuilder = ProjectMetadata.builder(project);
        mdBuilder.removeDataStream("logs-postgres-eu");
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), nullValue());
    }

    public void testDeleteDataStreamAlias() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-us", true), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-au")
        );

        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-au", true), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-eu"));

        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-eu", true), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), nullValue());
    }

    public void testDeleteDataStreamAliasMustExists() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-eu"));
        mdBuilder.put("logs-postgres", "logs-postgres-eu", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-us"));
        mdBuilder.put("logs-postgres", "logs-postgres-us", null, null);
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-au"));
        mdBuilder.put("logs-postgres", "logs-postgres-au", null, null);
        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-eu", "logs-postgres-us", "logs-postgres-au")
        );

        ProjectMetadata.Builder mdBuilder2 = ProjectMetadata.builder(project);
        expectThrows(ResourceNotFoundException.class, () -> mdBuilder2.removeDataStreamAlias("logs-mysql", "logs-postgres-us", true));
        assertThat(mdBuilder2.removeDataStreamAlias("logs-mysql", "logs-postgres-us", false), is(false));
    }

    public void testDataStreamWriteAlias() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null);

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));

        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null), is(true));

        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-replicated"));
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));
    }

    public void testDataStreamMultipleWriteAlias() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-foobar"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-barbaz"));
        mdBuilder.put("logs", "logs-foobar", true, null);
        mdBuilder.put("logs", "logs-barbaz", true, null);

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs").getWriteDataStream(), equalTo("logs-barbaz"));
        assertThat(project.dataStreamAliases().get("logs").getDataStreams(), containsInAnyOrder("logs-foobar", "logs-barbaz"));
    }

    public void testDataStreamWriteAliasUnset() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null);

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-replicated"));
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));

        mdBuilder = ProjectMetadata.builder(project);
        // Side check: null value isn't changing anything:
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(false));
        // Unset write flag
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", false, null), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));
    }

    public void testDataStreamWriteAliasChange() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        // change write flag:
        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", false, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", true, null), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-replicated"));
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );
    }

    public void testDataStreamWriteRemoveAlias() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        mdBuilder = ProjectMetadata.builder(project);
        assertThat(mdBuilder.removeDataStreamAlias("logs-postgres", "logs-postgres-primary", randomBoolean()), is(true));
        project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));
    }

    public void testDataStreamWriteRemoveDataStream() {
        ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-primary"));
        mdBuilder.put(DataStreamTestHelper.randomInstance("logs-postgres-replicated"));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-primary", true, null), is(true));
        assertThat(mdBuilder.put("logs-postgres", "logs-postgres-replicated", null, null), is(true));

        ProjectMetadata project = mdBuilder.build();
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), equalTo("logs-postgres-primary"));
        assertThat(
            project.dataStreamAliases().get("logs-postgres").getDataStreams(),
            containsInAnyOrder("logs-postgres-primary", "logs-postgres-replicated")
        );

        mdBuilder = ProjectMetadata.builder(project);
        mdBuilder.removeDataStream("logs-postgres-primary");
        project = mdBuilder.build();
        assertThat(project.dataStreams().keySet(), contains("logs-postgres-replicated"));
        assertThat(project.dataStreamAliases().get("logs-postgres"), notNullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getWriteDataStream(), nullValue());
        assertThat(project.dataStreamAliases().get("logs-postgres").getDataStreams(), containsInAnyOrder("logs-postgres-replicated"));
    }

    public void testReuseIndicesLookup() {
        String indexName = "my-index";
        String aliasName = "my-alias";
        String dataStreamName = "logs-mysql-prod";
        String dataStreamAliasName = "logs-mysql";
        ProjectMetadata previous = ProjectMetadata.builder(randomProjectIdOrDefault()).build();

        // Things that should change indices lookup
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata idx = DataStreamTestHelper.createFirstBackingIndex(dataStreamName).build();
            builder.put(idx, true);
            DataStream dataStream = newInstance(dataStreamName, List.of(idx.getIndex()));
            builder.put(dataStream);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            builder.put(dataStreamAliasName, dataStreamName, false, null);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            builder.put(dataStreamAliasName, dataStreamName, true, null);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            builder.put(
                IndexMetadata.builder(indexName)
                    .settings(settings(IndexVersion.current()))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.putAlias(AliasMetadata.builder(aliasName).build());
            builder.put(imBuilder);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build());
            builder.put(imBuilder);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            Settings.Builder sBuilder = Settings.builder()
                .put(builder.get(indexName).getSettings())
                .put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), true);
            imBuilder.settings(sBuilder.build());
            builder.put(imBuilder);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), not(sameInstance(project.getIndicesLookup())));
            previous = project;
        }

        // Things that shouldn't change indices lookup
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            imBuilder.numberOfReplicas(2);
            builder.put(imBuilder);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), sameInstance(project.getIndicesLookup()));
            previous = project;
        }
        {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(previous);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(builder.get(indexName));
            Settings.Builder sBuilder = Settings.builder()
                .put(builder.get(indexName).getSettings())
                .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), "val");
            imBuilder.settings(sBuilder.build());
            builder.put(imBuilder);
            ProjectMetadata project = builder.build();
            assertThat(previous.getIndicesLookup(), sameInstance(project.getIndicesLookup()));
            previous = project;
        }
    }

    public void testAliasedIndices() {
        int numAliases = randomIntBetween(32, 64);
        int numIndicesPerAlias = randomIntBetween(8, 16);

        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        ProjectMetadata project = builder.build();
        for (int i = 0; i < numAliases; i++) {
            String aliasName = "alias-" + i;
            Set<Index> result = project.aliasedIndices(aliasName);
            Index[] expected = IntStream.range(0, numIndicesPerAlias)
                .mapToObj(j -> aliasName + "-" + j)
                .map(name -> new Index(name, ClusterState.UNKNOWN_UUID))
                .toArray(Index[]::new);
            assertThat(result, containsInAnyOrder(expected));
        }

        // Add a new alias and index
        builder = ProjectMetadata.builder(project);
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
        project = builder.build();
        assertThat(project.aliasedIndices(), hasSize(numAliases + 1));
        assertThat(project.aliasedIndices(newAliasName), contains(new Index(newAliasName + "-1", ClusterState.UNKNOWN_UUID)));

        // Remove the new alias/index
        builder = ProjectMetadata.builder(project);
        {
            builder.remove(newAliasName + "-1");
        }
        project = builder.build();
        assertThat(project.aliasedIndices(), hasSize(numAliases));
        assertThat(project.aliasedIndices(newAliasName), empty());

        // Add a new alias that points to existing indices
        builder = ProjectMetadata.builder(project);
        {
            IndexMetadata.Builder imBuilder = new IndexMetadata.Builder(project.index("alias-1-0"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(project.index("alias-2-1"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(project.index("alias-3-2"));
            imBuilder.putAlias(new AliasMetadata.Builder(newAliasName));
            builder.put(imBuilder);
        }
        project = builder.build();
        assertThat(project.aliasedIndices(), hasSize(numAliases + 1));
        assertThat(
            project.aliasedIndices(newAliasName),
            containsInAnyOrder(
                new Index("alias-1-0", ClusterState.UNKNOWN_UUID),
                new Index("alias-2-1", ClusterState.UNKNOWN_UUID),
                new Index("alias-3-2", ClusterState.UNKNOWN_UUID)
            )
        );

        // Remove the new alias that points to existing indices
        builder = ProjectMetadata.builder(project);
        {
            IndexMetadata.Builder imBuilder = new IndexMetadata.Builder(project.index("alias-1-0"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(project.index("alias-2-1"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);

            imBuilder = new IndexMetadata.Builder(project.index("alias-3-2"));
            imBuilder.removeAlias(newAliasName);
            builder.put(imBuilder);
        }
        project = builder.build();
        assertThat(project.aliasedIndices(), hasSize(numAliases));
        assertThat(project.aliasedIndices(newAliasName), empty());
    }

    public void testHiddenAliasValidation() {
        final String hiddenAliasName = "hidden_alias";

        IndexMetadata hidden1 = buildIndexWithAlias("hidden1", hiddenAliasName, true, IndexVersion.current(), false);
        IndexMetadata hidden2 = buildIndexWithAlias("hidden2", hiddenAliasName, true, IndexVersion.current(), false);
        IndexMetadata hidden3 = buildIndexWithAlias("hidden3", hiddenAliasName, true, IndexVersion.current(), false);

        IndexMetadata nonHidden = buildIndexWithAlias("nonhidden1", hiddenAliasName, false, IndexVersion.current(), false);
        IndexMetadata unspecified = buildIndexWithAlias("nonhidden2", hiddenAliasName, null, IndexVersion.current(), false);

        {
            // Should be ok:
            projectWithIndices(hidden1, hidden2, hidden3);
        }

        {
            // Should be ok:
            if (randomBoolean()) {
                projectWithIndices(nonHidden, unspecified);
            } else {
                projectWithIndices(unspecified, nonHidden);
            }
        }

        {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> projectWithIndices(hidden1, hidden2, hidden3, nonHidden)
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
                () -> projectWithIndices(hidden1, hidden2, hidden3, unspecified)
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
                    projectWithIndices(nonHidden, unspecified, hiddenIndex);
                } else {
                    projectWithIndices(unspecified, nonHidden, hiddenIndex);
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

    public static final String SYSTEM_ALIAS_NAME = "system_alias";

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
            () -> projectWithIndices(currentVersionSystem, oldVersionSystem, regularIndex)
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
            () -> projectWithIndices(currentVersionSystem, regularIndex)
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
        projectWithIndices(oldVersionSystem, regularIndex);
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
        projectWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
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
        projectWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
    }

    public void testSystemAliasValidationAll8x() {
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, IndexVersion.current(), true);

        // Should be ok
        projectWithIndices(currentVersionSystem, currentVersionSystem2);
    }

    private void projectWithIndices(IndexMetadata... indices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        ProjectMetadata project;
        int numIndices = randomIntBetween(16, 32);
        {
            String[] definitions = randomMappingDefinitions.toArray(String[]::new);
            ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(
            project.indices().values().stream().map(IndexMetadata::mapping).collect(Collectors.toSet()),
            hasSize(project.getMappingsByHash().size())
        );

        // Add a new index with a new index with known mapping:
        MappingMetadata mapping = project.indices().get("index-" + randomInt(numIndices - 1)).mapping();
        MappingMetadata entry = project.getMappingsByHash().get(mapping.getSha256());
        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.put(
                IndexMetadata.builder("index-" + numIndices)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(mapping)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(project.getMappingsByHash().get(mapping.getSha256()), equalTo(entry));

        // Remove index and ensure mapping cache stays the same
        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.remove("index-" + numIndices);
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(project.getMappingsByHash().get(mapping.getSha256()), equalTo(entry));

        // Update a mapping of an index:
        IndexMetadata luckyIndex = project.index("index-" + randomInt(numIndices - 1));
        entry = project.getMappingsByHash().get(luckyIndex.mapping().getSha256());
        MappingMetadata updatedMapping = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of("mapping", "updated"));
        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.put(IndexMetadata.builder(luckyIndex).putMapping(updatedMapping));
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size() + 1));
        assertThat(project.getMappingsByHash().get(luckyIndex.mapping().getSha256()), equalTo(entry));
        assertThat(project.getMappingsByHash().get(updatedMapping.getSha256()), equalTo(updatedMapping));

        // Remove the index with updated mapping
        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.remove(luckyIndex.getIndex().getName());
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(project.getMappingsByHash().get(updatedMapping.getSha256()), nullValue());

        // Add an index with new mapping and then later remove it:
        MappingMetadata newMapping = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of("new", "mapping"));
        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.put(
                IndexMetadata.builder("index-" + numIndices)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .putMapping(newMapping)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            );
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size() + 1));
        assertThat(project.getMappingsByHash().get(newMapping.getSha256()), equalTo(newMapping));

        {
            ProjectMetadata.Builder mb = new ProjectMetadata.Builder(project);
            mb.remove("index-" + numIndices);
            project = mb.build();
        }
        assertThat(project.getMappingsByHash(), aMapWithSize(randomMappingDefinitions.size()));
        assertThat(project.getMappingsByHash().get(newMapping.getSha256()), nullValue());
    }

    public void testWithLifecycleState() {
        String indexName = "my-index";
        String indexUUID = randomAlphaOfLength(10);
        ProjectMetadata project1 = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        IndexMetadata index1 = project1.index(indexName);
        assertThat(project1.getIndicesLookup(), notNullValue());
        assertThat(index1.getLifecycleExecutionState(), sameInstance(LifecycleExecutionState.EMPTY_STATE));

        LifecycleExecutionState state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").build();
        ProjectMetadata project2 = project1.withLifecycleState(index1.getIndex(), state);
        IndexMetadata index2 = project2.index(indexName);

        // the indices lookups are the same object
        assertThat(project2.getIndicesLookup(), sameInstance(project1.getIndicesLookup()));

        // the lifecycle state and version were changed
        assertThat(index2.getLifecycleExecutionState().asMap(), is(state.asMap()));
        assertThat(index2.getVersion(), is(index1.getVersion() + 1));

        // but those are the only differences between the two
        IndexMetadata.Builder builder = IndexMetadata.builder(index2);
        builder.version(builder.version() - 1);
        builder.removeCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
        assertThat(index1, equalTo(builder.build()));

        // withLifecycleState returns the same reference if nothing changed
        ProjectMetadata project3 = project2.withLifecycleState(index2.getIndex(), state);
        assertThat(project3, sameInstance(project2));

        // withLifecycleState rejects a nonsense Index
        String randomUUID = randomValueOtherThan(indexUUID, () -> randomAlphaOfLength(10));
        expectThrows(IndexNotFoundException.class, () -> project1.withLifecycleState(new Index(indexName, randomUUID), state));
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.TIME_SERIES));
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.TIME_SERIES));
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.LOGSDB));
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), is(IndexMode.LOGSDB));
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), nullValue());
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
            ProjectMetadata p = ProjectMetadata.builder(randomProjectIdOrDefault())
                .put("component_template_1", componentTemplate)
                .put("index_template_1", indexTemplate)
                .build();
            assertThat(p.retrieveIndexModeFromTemplate(indexTemplate), nullValue());
        }
    }

    private static CreateIndexResult createIndices(int numIndices, int numBackingIndices, String dataStreamName) {
        // create some indices that do not back a data stream
        final List<Index> indices = new ArrayList<>();
        int lastIndexNum = randomIntBetween(9, 50);
        ProjectMetadata.Builder b = ProjectMetadata.builder(randomProjectIdOrDefault());
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

    private record CreateIndexResult(List<Index> indices, List<Index> backingIndices, ProjectMetadata project) {};

    public void testToXContent() throws IOException {
        final ProjectMetadata projectMetadata = prepareProjectMetadata();

        ToXContent.Params params = EMPTY_PARAMS;
        AbstractChunkedSerializingTestCase.assertChunkCount(projectMetadata, p -> expectedChunkCount(params, p));

        final BytesArray expected = new BytesArray(
            Strings.format(
                """
                    {
                      "templates": {},
                      "indices": {
                        "index-01": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "1",
                              "number_of_replicas": "1",
                              "uuid": "i3e800000001",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.1"
                          ],
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
                        "index-02": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 2,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "2",
                              "number_of_replicas": "0",
                              "uuid": "i7d000000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.2"
                          ],
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
                        },
                        "index-03": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "ibb800000003",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.3"
                          ],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "2": [],
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
                        },
                        ".ds-logs-ultron-2024.08.30-000001": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "1",
                              "number_of_replicas": "2",
                              "uuid": "d1000001",
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
                        ".ds-logs-ultron-2024.08.30-000002": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "d2000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "0": [],
                            "1": [],
                            "2": []
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
                      "index_template": {
                        "index_template": {
                          "template": {
                            "index_patterns": [
                              "index-*"
                            ],
                            "composed_of": [],
                            "priority": 10
                          }
                        }
                      },
                      "index-graveyard": {
                        "tombstones": []
                      },
                      "data_stream": {
                        "data_stream": {
                          "logs-ultron": {
                            "name": "logs-ultron",
                            "timestamp_field": {
                              "name": "@timestamp"
                            },
                            "indices": [
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000001",
                                "index_uuid": "d1000001"
                              },
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000002",
                                "index_uuid": "d2000002"
                              }
                            ],
                            "generation": 2,
                            "hidden": false,
                            "replicated": false,
                            "system": false,
                            "allow_custom_routing": false,
                            "settings" : { },
                            "failure_rollover_on_write": true,
                            "rollover_on_write": false
                          }
                        },
                        "data_stream_aliases": {}
                      }
                    }
                    """,
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current()
            )
        );
        final BytesReference actual = XContentHelper.toXContent(projectMetadata, XContentType.JSON, randomBoolean());
        assertToXContentEquivalent(expected, actual, XContentType.JSON);
    }

    public void testToXContentMultiProject() throws IOException {
        final ProjectMetadata projectMetadata = prepareProjectMetadata();

        ToXContent.Params params = new ToXContent.MapParams(Map.of("multi-project", "true"));
        AbstractChunkedSerializingTestCase.assertChunkCount(projectMetadata, params, p -> expectedChunkCount(params, p));

        final BytesArray expected = new BytesArray(
            Strings.format(
                """
                    {
                      "templates": {},
                      "indices": {
                        "index-01": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "1",
                              "number_of_replicas": "1",
                              "uuid": "i3e800000001",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.1"
                          ],
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
                        "index-02": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 2,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "2",
                              "number_of_replicas": "0",
                              "uuid": "i7d000000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.2"
                          ],
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
                        },
                        "index-03": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "ibb800000003",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [
                            "alias.3"
                          ],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "2": [],
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
                        },
                        ".ds-logs-ultron-2024.08.30-000001": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 1,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "1",
                              "number_of_replicas": "2",
                              "uuid": "d1000001",
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
                        ".ds-logs-ultron-2024.08.30-000002": {
                          "version": 1,
                          "transport_version" : "0",
                          "mapping_version": 1,
                          "settings_version": 1,
                          "aliases_version": 1,
                          "routing_num_shards": 3,
                          "state": "open",
                          "settings": {
                            "index": {
                              "hidden": "true",
                              "number_of_shards": "3",
                              "number_of_replicas": "1",
                              "uuid": "d2000002",
                              "version": {
                                "created": "%s"
                              }
                            }
                          },
                          "mappings": {},
                          "aliases": [],
                          "primary_terms": {
                            "0": 0,
                            "1": 0,
                            "2": 0
                          },
                          "in_sync_allocations": {
                            "0": [],
                            "1": [],
                            "2": []
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
                      "index_template": {
                        "index_template": {
                          "template": {
                            "index_patterns": [
                              "index-*"
                            ],
                            "composed_of": [],
                            "priority": 10
                          }
                        }
                      },
                      "index-graveyard": {
                        "tombstones": []
                      },
                      "data_stream": {
                        "data_stream": {
                          "logs-ultron": {
                            "name": "logs-ultron",
                            "timestamp_field": {
                              "name": "@timestamp"
                            },
                            "indices": [
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000001",
                                "index_uuid": "d1000001"
                              },
                              {
                                "index_name": ".ds-logs-ultron-2024.08.30-000002",
                                "index_uuid": "d2000002"
                              }
                            ],
                            "generation": 2,
                            "hidden": false,
                            "replicated": false,
                            "system": false,
                            "allow_custom_routing": false,
                            "settings" : { },
                            "failure_rollover_on_write": true,
                            "rollover_on_write": false
                          }
                        },
                        "data_stream_aliases": {}
                      }
                    }
                    """,
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current()
            )
        );
        final BytesReference actual = XContentHelper.toXContent(projectMetadata, XContentType.JSON, params, randomBoolean());
        assertToXContentEquivalent(expected, actual, XContentType.JSON);
    }

    private static ProjectMetadata prepareProjectMetadata() {
        final ProjectId projectId = randomUniqueProjectId();
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        for (int i = 1; i <= 3; i++) {
            builder.put(
                IndexMetadata.builder(Strings.format("index-%02d", i))
                    .settings(
                        indexSettings(IndexVersion.current(), i, i % 2).put(
                            IndexMetadata.SETTING_INDEX_UUID,
                            Strings.format("i%x%04d", (i * 1000 << 16), i)
                        )
                    )
                    .putAlias(AliasMetadata.builder(Strings.format("alias.%d", i)).build())
                    .build(),
                false
            );
        }
        builder.indexTemplates(
            Map.of("template", ComposableIndexTemplate.builder().indexPatterns(List.of("index-*")).priority(10L).build())
        );

        final String dataStreamName = "logs-ultron";
        final IndexMetadata backingIndex1 = DataStreamTestHelper.createBackingIndex(dataStreamName, 1, 1725000000000L)
            .settings(
                indexSettings(IndexVersion.current(), 1, 2).put("index.hidden", true)
                    .put(IndexMetadata.SETTING_INDEX_UUID, Strings.format("d%x", 0x1000001))
            )
            .build();
        final IndexMetadata backingIndex2 = DataStreamTestHelper.createBackingIndex(dataStreamName, 2, 1725025000000L)
            .settings(
                indexSettings(IndexVersion.current(), 3, 1).put("index.hidden", true)
                    .put(IndexMetadata.SETTING_INDEX_UUID, Strings.format("d%x", 0x2000002))
            )
            .build();
        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(backingIndex1.getIndex(), backingIndex2.getIndex())
        );
        builder.put(backingIndex1, false);
        builder.put(backingIndex2, false);
        builder.put(dataStream);

        final ProjectMetadata projectMetadata = builder.build();
        return projectMetadata;
    }

    static int expectedChunkCount(ToXContent.Params params, ProjectMetadata project) {
        final var context = Metadata.XContentContext.from(params);

        long chunkCount = 0;
        if (context == Metadata.XContentContext.API) {
            // 2 chunks wrapping "indices"" and one chunk per index
            chunkCount += 2 + project.indices().size();
        }

        // 2 chunks wrapping "templates" and one chunk per template
        chunkCount += 2 + project.templates().size();

        for (Metadata.ProjectCustom custom : project.customs().values()) {
            chunkCount += 2;  // open / close object
            if (custom instanceof ComponentTemplateMetadata componentTemplateMetadata) {
                chunkCount += checkChunkSize(custom, params, 2 + componentTemplateMetadata.componentTemplates().size());
            } else if (custom instanceof ComposableIndexTemplateMetadata composableIndexTemplateMetadata) {
                chunkCount += checkChunkSize(custom, params, 2 + composableIndexTemplateMetadata.indexTemplates().size());
            } else if (custom instanceof DataStreamMetadata dataStreamMetadata) {
                chunkCount += checkChunkSize(
                    custom,
                    params,
                    4 + dataStreamMetadata.dataStreams().size() + dataStreamMetadata.getDataStreamAliases().size()
                );
            } else if (custom instanceof IndexGraveyard indexGraveyard) {
                chunkCount += checkChunkSize(custom, params, 2 + indexGraveyard.getTombstones().size());
            } else if (custom instanceof IngestMetadata ingestMetadata) {
                chunkCount += checkChunkSize(custom, params, 2 + ingestMetadata.getPipelines().size());
            } else if (custom instanceof PersistentTasksCustomMetadata persistentTasksCustomMetadata) {
                chunkCount += checkChunkSize(custom, params, 3 + persistentTasksCustomMetadata.tasks().size());
            } else if (custom instanceof RepositoriesMetadata repositoriesMetadata) {
                chunkCount += checkChunkSize(custom, params, repositoriesMetadata.repositories().size());
            } else {
                // could be anything, we have to just try it
                chunkCount += count(custom.toXContentChunked(params));
            }
        }

        return Math.toIntExact(chunkCount);
    }

    private static class TestProjectCustomMetadata implements Metadata.ProjectCustom {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        @Override
        public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom previousState) {
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

}
