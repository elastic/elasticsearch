/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringIds;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.cluster.DiscoveryNodeMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.cluster.ClusterInfoResolver;
import org.elasticsearch.marvel.agent.resolver.cluster.ClusterStateNodeResolver;
import org.elasticsearch.marvel.agent.resolver.cluster.ClusterStateResolver;
import org.elasticsearch.marvel.agent.resolver.cluster.ClusterStatsResolver;
import org.elasticsearch.marvel.agent.resolver.cluster.DiscoveryNodeResolver;
import org.elasticsearch.marvel.agent.resolver.indices.IndexRecoveryResolver;
import org.elasticsearch.marvel.agent.resolver.indices.IndexStatsResolver;
import org.elasticsearch.marvel.agent.resolver.indices.IndicesStatsResolver;
import org.elasticsearch.marvel.agent.resolver.node.NodeStatsResolver;
import org.elasticsearch.marvel.agent.resolver.shards.ShardsResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ResolversRegistry implements Iterable<MonitoringIndexNameResolver> {

    private List<Registration> registrations;

    public ResolversRegistry(Settings settings) {
        List<Registration> resolvers = new ArrayList<>();
        registerElasticsearchResolvers(resolvers, settings);
        this.registrations = Collections.unmodifiableList(resolvers);
    }

    /** Registers default Elasticsearch resolvers used by the Marvel plugin. **/
    private void registerElasticsearchResolvers(List<Registration> resolvers, Settings settings) {
        Map<Class<? extends MonitoringDoc>, MonitoringIndexNameResolver<? extends MonitoringDoc>> map = new HashMap<>();
        map.put(ClusterInfoMonitoringDoc.class, new ClusterInfoResolver());
        map.put(ClusterStateNodeMonitoringDoc.class, new ClusterStateNodeResolver(settings));
        map.put(ClusterStateMonitoringDoc.class, new ClusterStateResolver(settings));
        map.put(ClusterStatsMonitoringDoc.class, new ClusterStatsResolver(settings));
        map.put(DiscoveryNodeMonitoringDoc.class, new DiscoveryNodeResolver());
        map.put(IndexRecoveryMonitoringDoc.class, new IndexRecoveryResolver(settings));
        map.put(IndexStatsMonitoringDoc.class, new IndexStatsResolver(settings));
        map.put(IndicesStatsMonitoringDoc.class, new IndicesStatsResolver(settings));
        map.put(NodeStatsMonitoringDoc.class, new NodeStatsResolver(settings));
        map.put(ShardMonitoringDoc.class, new ShardsResolver(settings));

        for (Map.Entry<Class<? extends MonitoringDoc>, MonitoringIndexNameResolver<? extends MonitoringDoc>> resolver : map.entrySet()) {
            resolvers.add(new Registration(MonitoringIds.ES.getId(), Version.CURRENT.toString(), resolver.getKey(), resolver.getValue()));
        }
    }

    public <T extends MonitoringDoc> MonitoringIndexNameResolver<MonitoringDoc> getResolver(T document) {
        return getResolver(document.getClass(), document.getMonitoringId(), document.getMonitoringVersion());
    }

    <T extends MonitoringDoc> MonitoringIndexNameResolver<T> getResolver(Class<? extends MonitoringDoc> clazz, String id, String version) {
        for (Registration registration : registrations) {
            if (registration.support(id, version, clazz)) {
                return (MonitoringIndexNameResolver<T>) registration.getResolver();
            }
        }
        throw new IllegalArgumentException("No resolver found for monitoring document [class=" + clazz.getName()
                + ", id=" + id + ", version=" + version + "]");
    }

    @Override
    public Iterator<MonitoringIndexNameResolver> iterator() {
        List<MonitoringIndexNameResolver> resolvers = new ArrayList<>(registrations.size());
        for (Registration registration : registrations) {
            resolvers.add(registration.getResolver());
        }
        return Collections.unmodifiableList(resolvers).iterator();
    }

    private static class Registration {

        private final String id;
        private final String version;
        private final Class<? extends MonitoringDoc> clazz;
        private final MonitoringIndexNameResolver<? extends MonitoringDoc> resolver;

        private Registration(String id, String version, Class<? extends MonitoringDoc> clazz,
                             MonitoringIndexNameResolver<? extends MonitoringDoc> resolver) {
            this.id = Objects.requireNonNull(id);
            this.version = Objects.requireNonNull(version);
            this.clazz = Objects.requireNonNull(clazz);
            this.resolver = Objects.requireNonNull(resolver);
        }

        public MonitoringIndexNameResolver<? extends MonitoringDoc> getResolver() {
            return resolver;
        }

        public boolean support(String id, String version, Class<? extends MonitoringDoc> documentClass) {
            return clazz.equals(documentClass) && this.id.equals(id) && this.version.equals(version);
        }

        @Override
        public String toString() {
            return "monitoring resolver [" + resolver.getClass().getName()
                    + "] registered for [class=" + clazz.getName() +
                    ", id=" + id +
                    ", version=" + version +
                    "]";
        }
    }
}
