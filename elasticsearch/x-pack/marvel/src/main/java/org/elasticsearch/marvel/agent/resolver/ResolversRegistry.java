/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.action.MonitoringBulkDoc;
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
import org.elasticsearch.marvel.agent.resolver.bulk.MonitoringBulkResolver;
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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.marvel.MonitoredSystem.ES;

public class ResolversRegistry implements Iterable<MonitoringIndexNameResolver> {

    private final List<Registration> registrations = new ArrayList<>();

    public ResolversRegistry(Settings settings) {
        // register built-in defaults resolvers
        registerBuiltIn(ES, settings);

        // register resolvers for monitored systems
        registerMonitoredSystem(MonitoredSystem.KIBANA, settings);
    }

    /**
     * Registers resolvers for elasticsearch documents collected by the monitoring plugin
     */
    private void registerBuiltIn(MonitoredSystem id, Settings settings) {
        registrations.add(resolveByClass(ClusterInfoMonitoringDoc.class, new ClusterInfoResolver()));
        registrations.add(resolveByClass(ClusterStateNodeMonitoringDoc.class, new ClusterStateNodeResolver(id, settings)));
        registrations.add(resolveByClass(ClusterStateMonitoringDoc.class, new ClusterStateResolver(id, settings)));
        registrations.add(resolveByClass(ClusterStatsMonitoringDoc.class, new ClusterStatsResolver(id, settings)));
        registrations.add(resolveByClass(DiscoveryNodeMonitoringDoc.class, new DiscoveryNodeResolver()));
        registrations.add(resolveByClass(IndexRecoveryMonitoringDoc.class, new IndexRecoveryResolver(id, settings)));
        registrations.add(resolveByClass(IndexStatsMonitoringDoc.class, new IndexStatsResolver(id, settings)));
        registrations.add(resolveByClass(IndicesStatsMonitoringDoc.class, new IndicesStatsResolver(id, settings)));
        registrations.add(resolveByClass(NodeStatsMonitoringDoc.class, new NodeStatsResolver(id, settings)));
        registrations.add(resolveByClass(ShardMonitoringDoc.class, new ShardsResolver(id, settings)));
    }

    /**
     * Registers resolvers for monitored systems
     */
    private void registerMonitoredSystem(MonitoredSystem id, Settings settings) {
        final MonitoringBulkResolver resolver =  new MonitoringBulkResolver(id, settings);
        registrations.add(resolveByClassSystemVersion(MonitoringBulkDoc.class, id, Version.CURRENT, resolver));
    }

    /**
     * @return a Resolver that is able to resolver the given monitoring document
     */
    public MonitoringIndexNameResolver getResolver(MonitoringDoc document) {
        for (Registration registration : registrations) {
            if (registration.support(document)) {
                return registration.resolver();
            }
        }
        throw new IllegalArgumentException("No resolver found for monitoring document");
    }

    @Override
    public Iterator<MonitoringIndexNameResolver> iterator() {
        return registrations.stream().map(Registration::resolver).iterator();
    }

    static Registration resolveByClass(Class<? extends MonitoringDoc> type, MonitoringIndexNameResolver resolver) {
        return new Registration(resolver, type::isInstance);
    }

    static Registration resolveByClassSystemVersion(Class<? extends MonitoringDoc> type, MonitoredSystem system, Version version,
                                                     MonitoringIndexNameResolver  resolver) {
        return new Registration(resolver, doc -> {
            try {
                if (type.isInstance(doc) == false) {
                    return false;
                }
                if (system != MonitoredSystem.fromSystem(doc.getMonitoringId())) {
                    return false;
                }
                return version == Version.fromString(doc.getMonitoringVersion());
            } catch (Exception e) {
                return false;
            }
        });
    }

    static class Registration {

        private final MonitoringIndexNameResolver resolver;
        private final Predicate<MonitoringDoc> predicate;

        Registration(MonitoringIndexNameResolver resolver, Predicate<MonitoringDoc> predicate) {
            this.resolver = Objects.requireNonNull(resolver);
            this.predicate = Objects.requireNonNull(predicate);
        }

        boolean support(MonitoringDoc document) {
            return predicate.test(document);
        }

        MonitoringIndexNameResolver resolver() {
            return resolver;
        }
    }
}
