/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SystemIndexAccessLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TransportGetAliasesActionTests extends ESTestCase {

    public void testPostProcess() {
        GetAliasesRequest request = new GetAliasesRequest();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE,
                SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest();
        request.replaceAliases("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE,
            SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE,
            SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("b").size(), equalTo(1));
    }

    public void testDeprecationWarningEmittedForTotalWildcard() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .fPut("c", Collections.singletonList(new AliasMetadata.Builder("d").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get(".b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequested() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(".b");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {".b"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequestedByAlias() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.NONE, null, EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningNotEmittedWhenSystemAccessAllowed() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.ALL, "", EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
    }

    /**
     * Ensures that deprecation warnings are not emitted when
     */
    public void testDeprecationWarningNotEmittedWhenOnlyNonsystemIndexRequested() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("c");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("c", Collections.singletonList(new AliasMetadata.Builder("d").build()))
            .build();
        final String[] concreteIndices = {"c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.NONE, "", EmptySystemIndices.INSTANCE);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
    }

    public void testDeprecationWarningEmittedWhenRequestingNonExistingAliasInSystemPattern() {
        ClusterState state = systemIndexTestClusterState();
        SystemIndices systemIndices = new SystemIndices(Collections.singletonMap(
            this.getTestName(),
            new SystemIndices.Feature(this.getTestName(), "test feature",
                Collections.singletonList(new SystemIndexDescriptor(".y", "an index that doesn't exist")))));

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .build();
        final String[] concreteIndices = {};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state,
                SystemIndexAccessLevel.NONE, null, systemIndices);
        assertThat(result.size(), equalTo(0));
        assertWarnings("this request accesses aliases with names reserved for system indices: [.y], but in a future major version, direct" +
            "access to system indices and their aliases will not be allowed");
    }

    public ClusterState systemIndexTestClusterState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("a").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .put(IndexMetadata.builder(".b").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
                    .system(true).putAlias(AliasMetadata.builder(".y")))
                .put(IndexMetadata.builder("c").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("d")))
                .build())
            .build();
    }


}
