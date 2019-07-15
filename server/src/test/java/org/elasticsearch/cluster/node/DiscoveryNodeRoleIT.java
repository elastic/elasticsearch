/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeRoleIT extends ESIntegTestCase {

    public static class AdditionalRolePlugin extends Plugin {

        public AdditionalRolePlugin() {

        }

        static final Setting<Boolean> NODE_ADDITIONAL_SETTING =
                Setting.boolSetting("node.additional", true, Setting.Property.NodeScope);

        static DiscoveryNodeRole ADDITIONAL_ROLE = new DiscoveryNodeRole("additional", "a") {

            @Override
            protected Setting<Boolean> roleSetting() {
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

    public void testExplicitlyHasAdditionalRole() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), true).build());
    }

    public void testDoesNotHaveAdditionalRole() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), false).build());
    }

    private void runTestNodeHasAdditionalRole(final Settings settings) {
        final String name = internalCluster().startNode(settings);
        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo(name).get();
        assertThat(response.getNodes(), hasSize(1));
        final Matcher<Iterable<? super DiscoveryNodeRole>> matcher;
        if (AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.get(settings)) {
            matcher = hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE);
        } else {
            matcher = not(hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE));
        }
        assertThat(response.getNodes().get(0).getNode().getRoles(), matcher);
    }

}
