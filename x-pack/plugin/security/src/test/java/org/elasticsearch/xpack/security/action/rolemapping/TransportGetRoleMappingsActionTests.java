/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authc.support.mapper.ProjectStateRoleMapper;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG;
import static org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransportGetRoleMappingsActionTests extends ESTestCase {

    private NativeRoleMappingStore store;
    private TransportGetRoleMappingsAction action;
    private AtomicReference<Set<String>> nativeNamesRef;
    private AtomicReference<Set<String>> clusterStateNamesRef;
    private List<ExpressionRoleMapping> nativeMappings;
    private Set<ExpressionRoleMapping> clusterStateMappings;

    @SuppressWarnings("unchecked")
    @Before
    public void setupMocks() {
        store = mock(NativeRoleMappingStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        ProjectStateRoleMapper projectStateRoleMapper = mock();
        action = new TransportGetRoleMappingsAction(mock(ActionFilters.class), transportService, store, projectStateRoleMapper);

        nativeNamesRef = new AtomicReference<>(null);
        clusterStateNamesRef = new AtomicReference<>(null);
        nativeMappings = Collections.emptyList();
        clusterStateMappings = Collections.emptySet();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 1;
            clusterStateNamesRef.set((Set<String>) args[0]);
            return clusterStateMappings;
        }).when(projectStateRoleMapper).getMappings(anySet());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            nativeNamesRef.set((Set<String>) args[0]);
            ActionListener<List<ExpressionRoleMapping>> listener = (ActionListener<List<ExpressionRoleMapping>>) args[1];
            listener.onResponse(nativeMappings);
            return null;
        }).when(store).getRoleMappings(nullable(Set.class), any(ActionListener.class));
    }

    public void testGetSingleRoleMappingNativeOnly() throws Exception {
        testGetMappings(List.of(mapping("everyone")), Collections.emptySet(), Set.of("everyone"), Set.of("everyone"), "everyone");
    }

    public void testGetMultipleNamedRoleMappingsNativeOnly() throws Exception {
        testGetMappings(
            List.of(mapping("admin"), mapping("engineering"), mapping("sales"), mapping("finance")),
            Collections.emptySet(),
            Set.of("admin", "engineering", "sales", "finance"),
            Set.of("admin", "engineering", "sales", "finance"),
            "admin",
            "engineering",
            "sales",
            "finance"
        );
    }

    public void testGetAllRoleMappingsNativeOnly() throws Exception {
        testGetMappings(
            List.of(mapping("admin"), mapping("engineering"), mapping("sales"), mapping("finance")),
            Collections.emptySet(),
            Set.of(),
            Set.of()
        );
    }

    public void testGetSingleRoleMappingClusterStateOnly() throws Exception {
        testGetMappings(List.of(), Set.of(mapping("everyone")), Set.of("everyone"), Set.of("everyone"), "everyone");
    }

    public void testGetMultipleNamedRoleMappingsClusterStateOnly() throws Exception {
        testGetMappings(
            List.of(),
            Set.of(mapping("admin"), mapping("engineering"), mapping("sales"), mapping("finance")),
            Set.of("admin", "engineering", "sales", "finance"),
            Set.of("admin", "engineering", "sales", "finance"),
            "admin",
            "engineering",
            "sales",
            "finance"
        );
    }

    public void testGetAllRoleMappingsClusterStateOnly() throws Exception {
        testGetMappings(
            List.of(),
            Set.of(mapping("admin"), mapping("engineering"), mapping("sales"), mapping("finance")),
            Set.of(),
            Set.of()
        );
    }

    public void testGetSingleRoleMappingBoth() throws Exception {
        testGetMappings(List.of(mapping("everyone")), Set.of(mapping("everyone")), Set.of("everyone"), Set.of("everyone"), "everyone");
    }

    public void testGetMultipleNamedRoleMappingsBoth() throws Exception {
        testGetMappings(
            List.of(mapping("admin"), mapping("engineering")),
            Set.of(mapping("sales"), mapping("finance")),
            Set.of("admin", "engineering", "sales", "finance"),
            Set.of("admin", "engineering", "sales", "finance"),
            "admin",
            "engineering",
            "sales",
            "finance"
        );
    }

    public void testGetAllRoleMappingsClusterBoth() throws Exception {
        testGetMappings(List.of(mapping("admin"), mapping("engineering")), Set.of(mapping("admin"), mapping("sales")), Set.of(), Set.of());
    }

    public void testGetSingleRoleMappingQueryWithReadOnlySuffix() throws Exception {
        testGetMappings(
            List.of(),
            Set.of(mapping("everyone")),
            // suffix not stripped for native store query
            Set.of("everyone" + READ_ONLY_ROLE_MAPPING_SUFFIX),
            // suffix is stripped for cluster state store
            Set.of("everyone"),
            "everyone" + READ_ONLY_ROLE_MAPPING_SUFFIX
        );

        testGetMappings(
            List.of(),
            Set.of(mapping("everyoneread-only-operator-mapping")),
            Set.of(
                "everyoneread-only-operator-mapping",
                "everyone-read-only-operator-mapping-",
                "everyone-read-only-operator-mapping-more"
            ),
            // suffix that is similar but not the same is not stripped
            Set.of(
                "everyoneread-only-operator-mapping",
                "everyone-read-only-operator-mapping-",
                "everyone-read-only-operator-mapping-more"
            ),
            "everyoneread-only-operator-mapping",
            "everyone-read-only-operator-mapping-",
            "everyone-read-only-operator-mapping-more"
        );

        testGetMappings(
            List.of(mapping("everyone")),
            Set.of(mapping("everyone")),
            // suffix not stripped for native store query
            Set.of("everyone" + READ_ONLY_ROLE_MAPPING_SUFFIX, "everyone"),
            // suffix is stripped for cluster state store
            Set.of("everyone"),
            "everyone" + READ_ONLY_ROLE_MAPPING_SUFFIX,
            "everyone"
        );
    }

    public void testClusterStateRoleMappingWithFallbackNameOmitted() throws ExecutionException, InterruptedException {
        testGetMappings(
            List.of(),
            Set.of(mapping("name_not_available_after_deserialization")),
            Set.of(),
            Set.of("name_not_available_after_deserialization"),
            Set.of("name_not_available_after_deserialization"),
            "name_not_available_after_deserialization"
        );

        testGetMappings(
            List.of(mapping("name_not_available_after_deserialization")),
            Set.of(mapping("name_not_available_after_deserialization")),
            Set.of(),
            Set.of("name_not_available_after_deserialization"),
            Set.of("name_not_available_after_deserialization"),
            "name_not_available_after_deserialization"
        );
    }

    private void testGetMappings(
        List<ExpressionRoleMapping> returnedNativeMappings,
        Set<ExpressionRoleMapping> returnedClusterStateMappings,
        Set<String> expectedNativeNames,
        Set<String> expectedClusterStateNames,
        String... names
    ) throws InterruptedException, ExecutionException {
        testGetMappings(
            returnedNativeMappings,
            returnedClusterStateMappings,
            returnedClusterStateMappings.stream().map(this::expectedClusterStateMapping).collect(Collectors.toSet()),
            expectedNativeNames,
            expectedClusterStateNames,
            names
        );
    }

    private void testGetMappings(
        List<ExpressionRoleMapping> returnedNativeMappings,
        Set<ExpressionRoleMapping> returnedClusterStateMappings,
        Set<ExpressionRoleMapping> expectedClusterStateMappings,
        Set<String> expectedNativeNames,
        Set<String> expectedClusterStateNames,
        String... names
    ) throws InterruptedException, ExecutionException {
        final PlainActionFuture<GetRoleMappingsResponse> future = new PlainActionFuture<>();
        final GetRoleMappingsRequest request = new GetRoleMappingsRequest();
        request.setNames(names);

        nativeMappings = returnedNativeMappings;
        clusterStateMappings = returnedClusterStateMappings;
        action.doExecute(mock(Task.class), request, future);
        assertThat(future.get(), notNullValue());
        List<ExpressionRoleMapping> combined = new ArrayList<>(returnedNativeMappings);
        combined.addAll(expectedClusterStateMappings);
        ExpressionRoleMapping[] actualMappings = future.get().mappings();
        assertThat(actualMappings, arrayContainingInAnyOrder(combined.toArray(new ExpressionRoleMapping[0])));
        assertThat(nativeNamesRef.get(), containsInAnyOrder(expectedNativeNames.toArray(new String[0])));
        assertThat(clusterStateNamesRef.get(), containsInAnyOrder(expectedClusterStateNames.toArray(new String[0])));
    }

    private ExpressionRoleMapping mapping(String name) {
        return new ExpressionRoleMapping(name, null, null, null, Map.of(), true);
    }

    private ExpressionRoleMapping expectedClusterStateMapping(ExpressionRoleMapping mapping) {
        return new ExpressionRoleMapping(
            mapping.getName() + READ_ONLY_ROLE_MAPPING_SUFFIX,
            null,
            null,
            null,
            Map.of(READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true),
            true
        );
    }
}
