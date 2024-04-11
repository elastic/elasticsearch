/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class DisableNativeRoleMappingsStoreTests extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(PrivateCustomPlugin.class);
        return plugins;
    }

    public void testPutRoleMappingDisallowed() {
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        nativeRoleMappingStore.putRoleMapping(new PutRoleMappingRequest(), future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertThat(e.getMessage(), containsString("Native role mapping management is disabled"));
    }

    public void testDeleteRoleMappingDisallowed() {
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        nativeRoleMappingStore.deleteRoleMapping(new DeleteRoleMappingRequest(), future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertThat(e.getMessage(), containsString("Native role mapping management is disabled"));
    }

    public static class PrivateCustomPlugin extends Plugin {

        public static final Setting<Boolean> NATIVE_ROLE_MAPPINGS_SETTING = Setting.boolSetting(
            "xpack.security.authc.native_role_mappings.enabled",
            true,
            Setting.Property.NodeScope
        );

        public PrivateCustomPlugin() {}

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(NATIVE_ROLE_MAPPINGS_SETTING.getKey(), false).build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(NATIVE_ROLE_MAPPINGS_SETTING);
        }
    }
}
