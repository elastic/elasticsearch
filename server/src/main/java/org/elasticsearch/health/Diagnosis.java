/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.health.HealthService.HEALTH_API_ID_PREFIX;

/**
 * Details a potential issue that was diagnosed by a {@link HealthService}.
 *
 * @param definition The definition of the diagnosis (e.g. message, helpURL)
 * @param affectedResources Optional list of "things" that are affected by this condition (e.g. shards, indices, or policies).
 */
public record Diagnosis(Definition definition, @Nullable List<Resource> affectedResources) implements ChunkedToXContent {

    /**
     * Represents a type of affected resource, together with the resources/abstractions that
     * are affected.
     */
    public static class Resource implements ChunkedToXContent {

        public static final String ID_FIELD = "id";
        public static final String NAME_FIELD = "name";

        public enum Type {
            INDEX("indices"),
            NODE("nodes"),
            SLM_POLICY("slm_policies"),
            ILM_POLICY("ilm_policies"),
            FEATURE_STATE("feature_states"),
            SNAPSHOT_REPOSITORY("snapshot_repositories");

            private final String displayValue;

            Type(String displayValue) {
                this.displayValue = displayValue;
            }
        }

        private final Type type;

        @Nullable
        private Collection<String> values;
        @Nullable
        private Collection<DiscoveryNode> nodes;

        public Resource(Type type, Collection<String> values) {
            if (type == Type.NODE) {
                throw new IllegalArgumentException("Nodes should be modelled using the dedicated constructor");
            }

            this.type = type;
            this.values = values;
        }

        public Resource(Collection<DiscoveryNode> nodes) {
            this.type = Type.NODE;
            this.nodes = nodes;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            var builder = ChunkedToXContent.builder(params);
            if (nodes != null) {
                return builder.array(type.displayValue, nodes.iterator(), node -> (b, p) -> {
                    b.startObject();
                    b.field(ID_FIELD, node.getId());
                    if (node.getName() != null) {
                        b.field(NAME_FIELD, node.getName());
                    }
                    return b.endObject();
                });
            } else {
                return builder.array(type.displayValue, values.toArray(String[]::new));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Resource resource = (Resource) o;
            return type == resource.type && Objects.equals(values, resource.values) && Objects.equals(nodes, resource.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, values, nodes);
        }

        public Type getType() {
            return type;
        }

        @Nullable
        public Collection<String> getValues() {
            return values;
        }

        @Nullable
        public Collection<DiscoveryNode> getNodes() {
            return nodes;
        }
    }

    /**
     * Details a diagnosis - cause and a potential action that a user could take to clear an issue identified by a {@link HealthService}.
     *
     * @param indicatorName The name of the health indicator service that will generate this diagnosis
     * @param id An identifier unique to this diagnosis across the health indicator that generates it
     * @param cause A description of the cause of the problem
     * @param action A description of the action to be taken to remedy the problem
     * @param helpURL Optional evergreen url to a help document
     */
    public record Definition(String indicatorName, String id, String cause, String action, String helpURL) {
        public String getUniqueId() {
            return HEALTH_API_ID_PREFIX + indicatorName + ":diagnosis:" + id;
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).object(ob -> {
            ob.append((b, p) -> {
                b.field("id", definition.getUniqueId());
                b.field("cause", definition.cause);
                b.field("action", definition.action);
                b.field("help_url", definition.helpURL);
                return b;
            });
            if (affectedResources != null && affectedResources.isEmpty() == false) {
                ob.object("affected_resources", affectedResources.iterator(), ChunkedToXContentBuilder::append);
            }
        });
    }
}
