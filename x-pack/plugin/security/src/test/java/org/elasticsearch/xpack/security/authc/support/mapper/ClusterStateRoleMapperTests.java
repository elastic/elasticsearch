/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;

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

public class ClusterStateRoleMapperTests extends ESTestCase {

    private ScriptService scriptService;
    private ClusterService clusterService;

    @Before
    public void setup() {
        scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        clusterService = mock(ClusterService.class);
    }

    public void testRegisterForClusterChangesIfEnabled() {
        Settings settings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", true).build();
        ClusterStateRoleMapper roleMapper = new ClusterStateRoleMapper(settings, scriptService, clusterService);
        verify(clusterService, times(1)).addListener(same(roleMapper));
    }

    public void testNoRegisterForClusterChangesIfNotEnabled() {
        Settings settings;
        if (randomBoolean()) {
            settings = Settings.EMPTY;
        } else {
            settings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", false).build();
        }
        new ClusterStateRoleMapper(settings, scriptService, clusterService);
        verifyNoInteractions(clusterService);
    }

    public void testRoleMappingMatching() throws Exception {
        UserRoleMapper.UserData userData = mock(UserRoleMapper.UserData.class);
        ExpressionModel expressionModel = mock(ExpressionModel.class);
        when(userData.asModel()).thenReturn(expressionModel);
        ExpressionRoleMapping mapping1 = mockExpressionRoleMapping(false, Set.of("role1"), expressionModel);
        ExpressionRoleMapping mapping2 = mockExpressionRoleMapping(true, Set.of("role2"));
        ExpressionRoleMapping mapping3 = mockExpressionRoleMapping(true, Set.of("role3"), expressionModel);
        RoleMappingMetadata roleMappingMetadata = new RoleMappingMetadata(Set.of(mapping1, mapping2, mapping3));
        ClusterState state = roleMappingMetadata.updateClusterState(ClusterState.builder(new ClusterName("elasticsearch")).build());
        when(clusterService.state()).thenReturn(state);
        {
            // the role mapper is enabled
            Settings settings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", true).build();
            ClusterStateRoleMapper roleMapper = new ClusterStateRoleMapper(settings, scriptService, clusterService);
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
            Settings settings;
            if (randomBoolean()) {
                settings = Settings.EMPTY;
            } else {
                settings = Settings.builder().put("xpack.security.authc.cluster_state_role_mappings.enabled", false).build();
            }
            ClusterStateRoleMapper roleMapper = new ClusterStateRoleMapper(settings, scriptService, clusterService);
            PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
            roleMapper.resolveRoles(userData, future);
            Set<String> roleNames = future.get();
            assertThat(roleNames, empty());
            verifyNoMoreInteractions(mapping1, mapping2, mapping3);
        }
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
