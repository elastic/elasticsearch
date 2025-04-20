/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TransportGetAliasesActionTests extends ESTestCase {

    public void testPostProcess() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        builder.put(IndexMetadata.builder("a").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0));
        builder.put(IndexMetadata.builder("b").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0));
        builder.put(IndexMetadata.builder("c").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0));
        ProjectMetadata projectMetadata = builder.build();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        Map<String, List<AliasMetadata>> aliases = Map.of("b", Collections.singletonList(new AliasMetadata.Builder("y").build()));
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            new String[] { "a", "b", "c" },
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        request.replaceAliases("y", "z");
        aliases = Map.of("b", Collections.singletonList(new AliasMetadata.Builder("y").build()));
        result = TransportGetAliasesAction.postProcess(
            request,
            new String[] { "a", "b", "c" },
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "y", "z");
        aliases = Map.of("b", Collections.singletonList(new AliasMetadata.Builder("y").build()));
        result = TransportGetAliasesAction.postProcess(
            request,
            new String[] { "a", "b", "c" },
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("b").size(), equalTo(1));
    }

    public void testDeprecationWarningEmittedForTotalWildcard() {
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        Map<String, List<AliasMetadata>> aliases = Map.of(
            ".b",
            Collections.singletonList(new AliasMetadata.Builder(".y").build()),
            "c",
            Collections.singletonList(new AliasMetadata.Builder("d").build())
        );
        final String[] concreteIndices = { "a", ".b", "c" };
        assertEquals(projectMetadata.findAliases(request.aliases(), concreteIndices), aliases);
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            concreteIndices,
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get(".b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.b], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequested() {
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        request.indices(".b");
        Map<String, List<AliasMetadata>> aliases = Map.of(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()));
        final String[] concreteIndices = { ".b" };
        assertEquals(projectMetadata.findAliases(request.aliases(), concreteIndices), aliases);
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            concreteIndices,
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.b], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequestedByAlias() {
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, ".y");
        Map<String, List<AliasMetadata>> aliases = Map.of(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()));
        final String[] concreteIndices = { "a", ".b", "c" };
        assertEquals(projectMetadata.findAliases(request.aliases(), concreteIndices), aliases);
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            concreteIndices,
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            null,
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.b], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
    }

    public void testDeprecationWarningNotEmittedWhenSystemAccessAllowed() {
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, ".y");
        Map<String, List<AliasMetadata>> aliases = Map.of(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()));
        final String[] concreteIndices = { "a", ".b", "c" };
        assertEquals(projectMetadata.findAliases(request.aliases(), concreteIndices), aliases);
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            concreteIndices,
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.ALL,
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
    }

    /**
     * Ensures that deprecation warnings are not emitted when
     */
    public void testDeprecationWarningNotEmittedWhenOnlyNonsystemIndexRequested() {
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();

        GetAliasesRequest request = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        request.indices("c");
        Map<String, List<AliasMetadata>> aliases = Map.of("c", Collections.singletonList(new AliasMetadata.Builder("d").build()));
        final String[] concreteIndices = { "c" };
        assertEquals(projectMetadata.findAliases(request.aliases(), concreteIndices), aliases);
        Map<String, List<AliasMetadata>> result = TransportGetAliasesAction.postProcess(
            request,
            concreteIndices,
            aliases,
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
    }

    public void testPostProcessDataStreamAliases() {
        var resolver = TestIndexNameExpressionResolver.newInstance();
        var tuples = List.of(new Tuple<>("logs-foo", 1), new Tuple<>("logs-bar", 1), new Tuple<>("logs-baz", 1));
        @FixForMultiProject(description = "update the helper method to random for non-default project")
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(tuples, List.of());
        var builder = Metadata.builder(clusterState.metadata());
        builder.put("logs", "logs-foo", null, null);
        builder.put("logs", "logs-bar", null, null);
        builder.put("secret", "logs-bar", null, null);
        final ProjectMetadata projectMetadata = ClusterState.builder(clusterState).metadata(builder).build().metadata().getProject();

        // return all all data streams with aliases
        var getAliasesRequest = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        var result = TransportGetAliasesAction.postProcess(resolver, getAliasesRequest, projectMetadata);
        assertThat(result.keySet(), containsInAnyOrder("logs-foo", "logs-bar"));
        assertThat(result.get("logs-foo"), contains(new DataStreamAlias("logs", List.of("logs-bar", "logs-foo"), null, null)));
        assertThat(
            result.get("logs-bar"),
            containsInAnyOrder(
                new DataStreamAlias("logs", List.of("logs-bar", "logs-foo"), null, null),
                new DataStreamAlias("secret", List.of("logs-bar"), null, null)
            )
        );

        // filter by alias name
        getAliasesRequest = new GetAliasesRequest(TEST_REQUEST_TIMEOUT, "secret");
        result = TransportGetAliasesAction.postProcess(resolver, getAliasesRequest, projectMetadata);
        assertThat(result.keySet(), containsInAnyOrder("logs-bar"));
        assertThat(result.get("logs-bar"), contains(new DataStreamAlias("secret", List.of("logs-bar"), null, null)));

        // filter by data stream:
        getAliasesRequest = new GetAliasesRequest(TEST_REQUEST_TIMEOUT).indices("logs-foo");
        result = TransportGetAliasesAction.postProcess(resolver, getAliasesRequest, projectMetadata);
        assertThat(result.keySet(), containsInAnyOrder("logs-foo"));
        assertThat(result.get("logs-foo"), contains(new DataStreamAlias("logs", List.of("logs-bar", "logs-foo"), null, null)));
    }

    public void testNetNewSystemIndicesDontErrorWhenNotRequested() {
        GetAliasesRequest aliasesRequest = new GetAliasesRequest(TEST_REQUEST_TIMEOUT);
        // `.b` will be the "net new" system index this test case
        ProjectMetadata projectMetadata = systemIndexTestProjectMetadata();
        String[] concreteIndices;

        SystemIndexDescriptor netNewDescriptor = SystemIndexDescriptor.builder()
            .setIndexPattern(".b*")
            .setAliasName(".y")
            .setPrimaryIndex(".b")
            .setDescription(this.getTestName())
            .setMappings("{\"_meta\":  {\"version\":  \"1.0.0\", \"" + SystemIndexDescriptor.VERSION_META_KEY + "\": 0}}")
            .setSettings(Settings.EMPTY)
            .setOrigin(this.getTestName())
            .setNetNew()
            .build();
        SystemIndices systemIndices = new SystemIndices(
            Collections.singletonList(
                new SystemIndices.Feature(this.getTestName(), "test feature", Collections.singletonList(netNewDescriptor))
            )
        );

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(
            threadContext,
            systemIndices
        );
        concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(projectMetadata, aliasesRequest);

        Map<String, List<AliasMetadata>> finalResponse = TransportGetAliasesAction.postProcess(
            aliasesRequest,
            concreteIndices,
            Map.of(),
            projectMetadata,
            SystemIndexAccessLevel.NONE,
            threadContext,
            systemIndices
        );

        // The real assertion is that the above `postProcess` call doesn't throw
        assertFalse(finalResponse.containsKey(".b"));
    }

    public ProjectMetadata systemIndexTestProjectMetadata() {
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("a").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .put(
                IndexMetadata.builder(".b")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .system(true)
                    .putAlias(AliasMetadata.builder(".y"))
            )
            .put(
                IndexMetadata.builder("c")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("d"))
            )
            .build();
    }

}
