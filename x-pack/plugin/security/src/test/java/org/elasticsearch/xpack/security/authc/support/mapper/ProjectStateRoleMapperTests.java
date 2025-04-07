/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ProjectStateRoleMapperTests extends ESTestCase {

    private ScriptService scriptService;
    private ClusterService clusterService;
    private Settings enabledSettings;
    private Settings disabledSettings;

    @Before
    public void setup() {
        scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine(Settings.EMPTY)),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        clusterService = mock(ClusterService.class);
        disabledSettings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", false).build();
        if (randomBoolean()) {
            enabledSettings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", true).build();
        } else {
            // the cluster state role mapper is enabled by default
            enabledSettings = Settings.EMPTY;
        }
    }

    public void testRegisterForClusterChangesIfEnabled() {
        ProjectStateRoleMapper roleMapper = new ProjectStateRoleMapper(
            enabledSettings,
            scriptService,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        verify(clusterService, times(1)).addListener(same(roleMapper));
    }

    public void testNoRegisterForClusterChangesIfNotEnabled() {
        new ProjectStateRoleMapper(disabledSettings, scriptService, clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        verifyNoInteractions(clusterService);
    }

    public void testRoleResolving() throws Exception {
        UserRoleMapper.UserData userData = mock(UserRoleMapper.UserData.class);
        ExpressionModel expressionModel = mock(ExpressionModel.class);
        when(userData.asModel()).thenReturn(expressionModel);
        ExpressionRoleMapping mapping1 = mockExpressionRoleMapping(false, Set.of("role1"), expressionModel);
        ExpressionRoleMapping mapping2 = mockExpressionRoleMapping(true, Set.of("role2"));
        ExpressionRoleMapping mapping3 = mockExpressionRoleMapping(true, Set.of("role3"), expressionModel);
        RoleMappingMetadata roleMappingMetadata = new RoleMappingMetadata(Set.of(mapping1, mapping2, mapping3));

        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(roleMappingMetadata.updateProject(projectMetadata))
            .build();
        when(clusterService.state()).thenReturn(state);
        {
            // the role mapper is enabled
            ProjectStateRoleMapper roleMapper = new ProjectStateRoleMapper(
                enabledSettings,
                scriptService,
                clusterService,
                TestProjectResolvers.singleProject(projectId)
            );
            PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
            roleMapper.resolveRoles(userData, future);
            Set<String> roleNames = future.get();
            assertThat(roleNames, contains("role3"));
            verify(mapping1).isEnabled();
            verify(mapping2).isEnabled();
            verify(mapping3).isEnabled();
            verify(mapping2).getExpression();
            verify(mapping3).getExpression();
            verify(mapping3).getRoleNames(same(scriptService), same(expressionModel));
            verifyNoMoreInteractions(mapping1, mapping2, mapping3);
        }
        {
            // but if the role mapper is disabled, NO roles are resolved
            ProjectStateRoleMapper roleMapper = new ProjectStateRoleMapper(
                disabledSettings,
                scriptService,
                clusterService,
                TestProjectResolvers.singleProject(projectId)
            );
            PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
            roleMapper.resolveRoles(userData, future);
            Set<String> roleNames = future.get();
            assertThat(roleNames, empty());
            verifyNoMoreInteractions(mapping1, mapping2, mapping3);
        }
    }

    public void testRoleMappingChangesTriggerRealmCacheClear() {
        CachingRealm mockRealm = mock(CachingRealm.class);

        ProjectStateRoleMapper roleMapper = new ProjectStateRoleMapper(
            enabledSettings,
            scriptService,
            clusterService,
            TestProjectResolvers.allProjects()
        );
        roleMapper.clearRealmCacheOnChange(mockRealm);
        ExpressionRoleMapping mapping1 = mockExpressionRoleMapping(true, Set.of("role"), mock(ExpressionModel.class));
        ExpressionModel model2 = mock(ExpressionModel.class);
        ExpressionRoleMapping mapping2 = mockExpressionRoleMapping(true, Set.of("role"), model2);
        ExpressionRoleMapping mapping3 = mockExpressionRoleMapping(true, Set.of("role3"), model2);

        final var projectId = randomProjectIdOrDefault();
        final Metadata.Builder metadataBuilder = new Metadata.Builder().put(ProjectMetadata.builder(projectId));

        for (int i = randomIntBetween(0, 5); i > 0; i--) {
            metadataBuilder.put(ProjectMetadata.builder(randomUniqueProjectId()));
        }

        ClusterState initialState = ClusterState.builder(new ClusterName("elasticsearch")).metadata(metadataBuilder).build();

        final BiFunction<ClusterState, RoleMappingMetadata, ClusterState> updateMetadata = (inputState, roleMappingMetadata) -> {
            final ProjectMetadata updatedProject = roleMappingMetadata.updateProject(inputState.metadata().getProject(projectId));
            return ClusterState.builder(inputState).putProjectMetadata(updatedProject).build();
        };
        RoleMappingMetadata roleMappingMetadata1 = new RoleMappingMetadata(Set.of(mapping1));
        ClusterState state1 = updateMetadata.apply(initialState, roleMappingMetadata1);
        roleMapper.clusterChanged(new ClusterChangedEvent("test", initialState, state1));
        verify(mockRealm, times(1)).expireAll();

        RoleMappingMetadata roleMappingMetadata2 = new RoleMappingMetadata(Set.of(mapping2));
        ClusterState state2 = updateMetadata.apply(state1, roleMappingMetadata2);
        roleMapper.clusterChanged(new ClusterChangedEvent("test", state1, state2));
        verify(mockRealm, times(2)).expireAll();

        RoleMappingMetadata roleMappingMetadata3 = new RoleMappingMetadata(Set.of(mapping3));
        ClusterState state3 = updateMetadata.apply(state2, roleMappingMetadata3);
        roleMapper.clusterChanged(new ClusterChangedEvent("test", state2, state3));
        verify(mockRealm, times(3)).expireAll();

        RoleMappingMetadata roleMappingMetadata4 = new RoleMappingMetadata(Set.of(mapping2, mapping3));
        ClusterState state4 = updateMetadata.apply(state3, roleMappingMetadata4);
        roleMapper.clusterChanged(new ClusterChangedEvent("test", state3, state4));
        verify(mockRealm, times(4)).expireAll();
    }

    private ExpressionRoleMapping mockExpressionRoleMapping(boolean enabled, Set<String> roleNames, ExpressionModel... matchingModels) {
        ExpressionRoleMapping mapping = mock(ExpressionRoleMapping.class);
        when(mapping.isEnabled()).thenReturn(enabled);
        RoleMapperExpression roleMapperExpression = mock(RoleMapperExpression.class);
        when(mapping.getExpression()).thenReturn(roleMapperExpression);
        doAnswer(invocation -> {
            ExpressionModel expressionModel = (ExpressionModel) invocation.getArguments()[0];
            for (ExpressionModel matchingModel : matchingModels) {
                if (expressionModel.equals(matchingModel)) {
                    return true;
                }
            }
            return false;
        }).when(roleMapperExpression).match(any(ExpressionModel.class));
        doAnswer(invocation -> {
            ExpressionModel expressionModel = (ExpressionModel) invocation.getArguments()[1];
            for (ExpressionModel matchingModel : matchingModels) {
                if (expressionModel.equals(matchingModel)) {
                    return roleNames;
                }
            }
            return Set.of();
        }).when(mapping).getRoleNames(same(scriptService), any(ExpressionModel.class));
        return mapping;
    }
}
