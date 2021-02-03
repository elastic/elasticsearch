/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeRoleIT extends ESIntegTestCase {

    public static class AdditionalRolePlugin extends Plugin {

        public AdditionalRolePlugin() {

        }

        static final Setting<Boolean> NODE_ADDITIONAL_SETTING =
                Setting.boolSetting("node.additional", true, Property.Deprecated, Property.NodeScope);

        static DiscoveryNodeRole ADDITIONAL_ROLE = new DiscoveryNodeRole("additional", "a") {

            @Override
            public Setting<Boolean> legacySetting() {
                return NODE_ADDITIONAL_SETTING;
            }

        };

        @Override
        public Set<DiscoveryNodeRole> getRoles() {
            return Set.of(ADDITIONAL_ROLE);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(NODE_ADDITIONAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AdditionalRolePlugin.class);
    }

    public void testDefaultHasAdditionalRole() {
        runTestNodeHasAdditionalRole(Settings.EMPTY);
    }

    public void testExplicitlyHasAdditionalRoleUsingLegacySetting() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), true).build());
    }

    public void testExplicitlyHasAdditionalRoles() {
        runTestNodeHasAdditionalRole(addRoles(Set.of(AdditionalRolePlugin.ADDITIONAL_ROLE)));
    }

    public void testDoesNotHaveAdditionalRoleUsingLegacySetting() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), false).build());
    }

    public void testExplicitlyDoesNotHaveAdditionalRole() {
        runTestNodeHasAdditionalRole(removeRoles(Set.of(AdditionalRolePlugin.ADDITIONAL_ROLE)));
    }

    private void runTestNodeHasAdditionalRole(final Settings settings) {
        final String name = internalCluster().startNode(settings);
        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo(name).get();
        assertThat(response.getNodes(), hasSize(1));
        final Matcher<Iterable<? super DiscoveryNodeRole>> matcher;
        if (DiscoveryNode.hasRole(settings, AdditionalRolePlugin.ADDITIONAL_ROLE)) {
            matcher = hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE);
        } else {
            matcher = not(hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE));
        }
        assertThat(response.getNodes().get(0).getNode().getRoles(), matcher);
    }

}
