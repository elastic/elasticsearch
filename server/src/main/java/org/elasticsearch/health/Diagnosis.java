/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.health.HealthService.HEALTH_API_ID_PREFIX;

/**
 * Details a potential issue that was diagnosed by a {@link HealthService}.
 *
 * @param definition The definition of the diagnosis (e.g. message, helpURL)
 * @param affectedResources Optional list of "things" that are affected by this condition (e.g. shards, indices, or policies).
 */
public record Diagnosis(Definition definition, @Nullable List<Resource> affectedResources) implements ToXContentObject {

    /**
     * Represents a type of affected resource, together with the resources/abstractions that
     * are affected.
     */
    public static class Resource implements ToXContentFragment {

        public static final String ID_FIELD = "id";
        public static final String NAME_FIELD = "name";

        public enum Type {
            INDEX("indices"),
            NODE("nodes"),
            SLM_POLICY("slm_policies"),
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (nodes != null) {
                // we report both node ids and names so we need a bit of structure
                builder.startArray(type.displayValue);
                for (DiscoveryNode node : nodes) {
                    builder.startObject();
                    builder.field(ID_FIELD, node.getId());
                    if (node.getName() != null) {
                        builder.field(NAME_FIELD, node.getName());
                    }
                    builder.endObject();
                }
                builder.endArray();
            } else {
                builder.field(type.displayValue, values);
            }
            return builder;
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
    public record Definition(String indicatorName, String id, String cause, String action, String helpURL) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", HEALTH_API_ID_PREFIX + definition.indicatorName + ":diagnosis:" + definition.id);
        builder.field("cause", definition.cause);
        builder.field("action", definition.action);

        if (affectedResources != null && affectedResources.size() > 0) {
            builder.startObject("affected_resources");
            for (Resource affectedResource : affectedResources) {
                affectedResource.toXContent(builder, params);
            }
            builder.endObject();
        }

        builder.field("help_url", definition.helpURL);
        return builder.endObject();
    }
}
