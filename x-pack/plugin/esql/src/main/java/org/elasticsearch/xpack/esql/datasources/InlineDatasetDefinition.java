/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A dataset definition a query carries instead of naming one that is registered: the data source it reads through,
 * and the definition itself as the fields a registration would have carried.
 * <p>
 * <b>Why it travels.</b> A data node does not read a source under the configuration the plan arrived with — it
 * rebuilds that configuration from its own cluster state ({@link DatasetConfigResolver}), because a plan is not
 * trusted to have been built by a coordinator the caller was allowed to use. Rebuilding needs the whole definition,
 * not a name: {@link DatasetRewriter#datasetConfig} takes a {@code Dataset} and the {@code DataSource} behind it, and
 * the settings it produces depend on the declared mapping ({@link PartitionSpec#alignWithMapping}). So the definition
 * rides here while the credentials stay where they always were, in the node's own data source registry. What the plan
 * decides is still only what it decides for a registered dataset: row shaping.
 * <p>
 * <b>Why it is not keyed by anything that wrote it.</b> The node reads a definition, and a definition is a definition
 * whoever typed it. Keying this on the syntax that produced it would make the rebind branch on where a relation came
 * from, and then every stage that touches a relation would be able to.
 *
 * @param dataSource the registered data source the definition reads through, resolved against the reading node's own
 *                   registry; never a pattern and never a list
 * @param definition the definition's own fields, in the shape an XContent parse of a registration body produces —
 *                   {@code resource}, {@code settings}, {@code mappings} and the rest, minus the data source
 */
public record InlineDatasetDefinition(String dataSource, Map<String, Object> definition) implements Writeable {

    /** The key the data source occupies in a registration body, which this record carries separately. */
    private static final String DATA_SOURCE_FIELD = "data_source";

    /** The version from which a node can read a definition off the wire. */
    private static final TransportVersion SUPPORTED_FROM = TransportVersion.fromName("esql_inline_dataset_definition");

    public InlineDatasetDefinition {
        Objects.requireNonNull(dataSource, "dataSource");
        definition = Map.copyOf(Objects.requireNonNull(definition, "definition"));
    }

    /**
     * Refuses {@code plan} if it carries a definition and some node in the cluster could not carry one, which is
     * decided here rather than where the plan is serialized: a leaf is read on whichever nodes hold its files, so a
     * refusal at the serializer would fail part of the query and leave the rest of it reading, and which nodes those
     * are is not known while the plan is still being analyzed.
     *
     * @param minimumVersion the oldest transport version in this cluster, so a plan that passes has no target it
     *                       cannot be sent to
     */
    public static void ensureEveryDefinitionCanTravel(LogicalPlan plan, TransportVersion minimumVersion) {
        if (minimumVersion.supports(SUPPORTED_FROM)) {
            return;
        }
        plan.forEachDown(UnresolvedExternalRelation.class, relation -> {
            if (relation.inlineDefinition() != null) {
                throw new VerificationException(
                    "a dataset definition carried by a query is not supported on all nodes in the cluster; "
                        + "retry after the upgrade, or register the dataset"
                );
            }
        });
    }

    public static InlineDatasetDefinition readFrom(StreamInput in) throws IOException {
        String dataSource = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, Object> definition = (Map<String, Object>) in.readGenericValue();
        return new InlineDatasetDefinition(dataSource, definition);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(dataSource);
        out.writeGenericValue(definition);
    }

    /**
     * The definition as the request a registration would have made, so that one validator serves both and the two
     * cannot come to disagree about what a definition may say. The name a registration takes from its URL path is the
     * data source's here: it is the only name an inline read has, and it is the name the read is authorized under.
     *
     * @throws IllegalArgumentException if the definition is not a shape a registration body could have had
     */
    public PutDatasetAction.Request asRegistrationRequest(TimeValue timeout) {
        if (definition.containsKey(DATA_SOURCE_FIELD)) {
            throw new IllegalArgumentException(
                "a dataset definition read inline takes its data source from its first argument; remove ["
                    + DATA_SOURCE_FIELD
                    + "] from the definition"
            );
        }
        Map<String, Object> body = new LinkedHashMap<>(definition);
        body.put(DATA_SOURCE_FIELD, dataSource);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(body);
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
            ) {
                return PutDatasetAction.Request.fromXContent(parser, timeout, timeout, dataSource);
            }
        } catch (IOException e) {
            // JSON over an in-memory map and string; an IOException here is a broken invariant, not input.
            throw new IllegalStateException("could not read an inline dataset definition", e);
        }
    }
}
