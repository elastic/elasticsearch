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

package org.elasticsearch.plugins.repository;

import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collection;
import java.util.Set;

public class PluginResolver {

    private final PluginRepository local;
    private final PluginRepository remote;

    public PluginResolver(PluginRepository local, PluginRepository remote) {
        this.local = local;
        this.remote = remote;
    }

    public Resolution resolve(PluginDescriptor plugin) {
        // Resolves the plugin with its dependencies
        Resolution report = new Resolution();
        doResolve(plugin, report);

        return report;
    }

    private void doResolve(PluginDescriptor plugin, Resolution report) {
        if ((plugin != null) && (report != null)) {

            // Try to resolve the plugin with the local repository
            PluginDescriptor resolved = local.find(plugin.organisation(), plugin.name(), plugin.version());
            if (resolved != null) {
                // Exact plugin is already installed
                report.addToExistings(resolved);
            }

            // Try to find similar plugin (but with different versions) in the local repository
            if (resolved == null) {
                Collection<PluginDescriptor> existing = local.find(plugin.name());
                if ((existing != null) && (!existing.isEmpty())) {
                    resolved = existing.iterator().next();
                    report.addtoConflicts(resolved, plugin);
                }
            }

            // Try to resolve the plugin with the remote repository
            if (resolved == null) {
                resolved = remote.find(plugin.organisation(), plugin.name(), plugin.version());
                if (resolved != null) {
                    report.addToAdditions(resolved);
                }
            }

            if (resolved == null) {
                report.addToMissings(plugin);
            }

            if (resolved != null) {

                if (report.resolved() == null) {
                    report.resolved(resolved);
                }

                if (resolved.dependencies() != null) {
                    for (PluginDescriptor dependency : resolved.dependencies()) {
                        if (!report.contains(dependency)) {
                            doResolve(dependency, report);
                        }
                    }
                }
            }
        }
    }


    public class Resolution {

        private PluginDescriptor resolved = null;

        private Set<PluginDescriptor> additions = Sets.newHashSet();
        private Set<PluginDescriptor> existings = Sets.newHashSet();
        private Set<PluginDescriptor> missings = Sets.newHashSet();
        private Set<Tuple<PluginDescriptor, PluginDescriptor>> conflicts = Sets.newHashSet();

        public PluginDescriptor resolved() {
            return resolved;
        }

        public void resolved(PluginDescriptor resolved) {
            this.resolved = resolved;
        }

        public Set<PluginDescriptor> additions() {
            return additions;
        }

        public void addToAdditions(PluginDescriptor plugin) {
            additions.add(plugin);
        }

        public Set<PluginDescriptor> existings() {
            return existings;
        }

        public void addToExistings(PluginDescriptor plugin) {
            existings.add(plugin);
        }

        public Set<PluginDescriptor> missings() {
            return missings;
        }

        public void addToMissings(PluginDescriptor plugin) {
            missings.add(plugin);
        }

        public Set<Tuple<PluginDescriptor, PluginDescriptor>> conflicts() {
            return conflicts;
        }

        public void addtoConflicts(PluginDescriptor existing, PluginDescriptor addition) {
            conflicts.add(new Tuple<PluginDescriptor, PluginDescriptor>(existing, addition));
        }

        public boolean contains(PluginDescriptor plugin) {
            if (additions.contains(plugin) || existings.contains(plugin) || missings.contains(plugin)) {
                return true;
            }
            for (Tuple<PluginDescriptor, PluginDescriptor> tuple : conflicts) {
                if (tuple.v1().equals(plugin)) {
                    return true;
                }
                if (tuple.v2().equals(plugin)) {
                    return true;
                }
            }
            return false;
        }
    }
}
