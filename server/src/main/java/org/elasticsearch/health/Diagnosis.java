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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collection;
import java.util.Collections;
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
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            final Iterator<? extends ToXContent> valuesIterator;
            if (nodes != null) {
                valuesIterator = Iterators.map(nodes.iterator(), node -> (builder, params) -> {
                    builder.startObject();
                    builder.field(ID_FIELD, node.getId());
                    if (node.getName() != null) {
                        builder.field(NAME_FIELD, node.getName());
                    }
                    builder.endObject();
                    return builder;
                });
            } else {
                valuesIterator = Iterators.map(values.iterator(), value -> (builder, params) -> builder.value(value));
            }
            return ChunkedToXContentHelper.array(type.displayValue, valuesIterator);
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

    private boolean hasResources() {
        return affectedResources != null && affectedResources.isEmpty() == false;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(ChunkedToXContentHelper.chunk((builder, params) -> {
            builder.startObject();
            builder.field("id", definition.getUniqueId());
            builder.field("cause", definition.cause);
            builder.field("action", definition.action);
            builder.field("help_url", definition.helpURL);

            if (hasResources()) {
                // don't want to have a new chunk & nested iterator for this, so we start the object here
                builder.startObject("affected_resources");
            }
            return builder;
        }),
            hasResources()
                ? Iterators.flatMap(affectedResources.iterator(), s -> s.toXContentChunked(outerParams))
                : Collections.emptyIterator(),
            ChunkedToXContentHelper.chunk((b, p) -> {
                if (hasResources()) {
                    b.endObject();
                }
                return b.endObject();
            })
        );
    }
}
