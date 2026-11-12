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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Which dataset an external source reads: one slot, either a registered dataset's {@link Named name} or an
 * {@link Inline} definition the query supplied. The reading node resolves whichever arrived.
 * <p>
 * The definition travels because a data node rebuilds a read's configuration from its own cluster state rather than
 * trusting what the plan arrived with, and rebuilding needs the whole definition: see
 * {@link DatasetRewriter#datasetConfig} and {@link PartitionSpec#alignWithMapping}. What does not travel is the data
 * source's settings — the {@link Inline} arm carries its <i>name</i>, and the credentials behind it are looked up in
 * the receiving node's own registry.
 */
public sealed interface DatasetReference extends Writeable {

    /** One wording for the two sites that refuse an inline arm, so a user sees the same sentence from either. */
    String NOT_EVERYWHERE = "a dataset definition carried by a query is not supported on all nodes in the cluster; "
        + "retry after the upgrade, or register the dataset";

    /** The version from which this slot is a reference; before it, the slot held a name and nothing else. */
    TransportVersion DATASET_REFERENCE = TransportVersion.fromName("esql_dataset_reference");

    /** The version from which the slot exists at all. A stream older than this carries no dataset behind a source. */
    TransportVersion DATASET_NAME = TransportVersion.fromName("esql_external_dataset_name");

    /** The name a diagnostic, a telemetry label or a log line uses for this read. */
    String describe();

    /** A dataset registered in cluster state, which every node looks up under this name. */
    record Named(String name) implements DatasetReference {

        private static final byte TAG = 0;

        public Named {
            Objects.requireNonNull(name, "name");
        }

        @Override
        public String describe() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TAG);
            out.writeString(name);
        }
    }

    /**
     * A definition the query supplied, read through a registered data source. The definition's own fields are in the
     * shape an XContent parse of a registration body produces — {@code resource}, {@code settings}, {@code mappings}
     * and the rest — minus the data source, which this names separately.
     *
     * @param dataSource the registered data source, resolved against the reading node's registry; never a pattern
     * @param definition the definition's fields, as a registration body would have carried them
     */
    record Inline(String dataSource, Map<String, Object> definition) implements DatasetReference {

        private static final byte TAG = 1;

        /** The key a registration body gives the data source, which this arm carries separately. */
        private static final String DATA_SOURCE_FIELD = "data_source";

        public Inline {
            Objects.requireNonNull(dataSource, "dataSource");
            definition = Map.copyOf(Objects.requireNonNull(definition, "definition"));
        }

        @Override
        public String describe() {
            return dataSource;
        }

        /** Field names only: the values are the caller's text, and a record's generated toString would render them. */
        @Override
        public String toString() {
            return "Inline[dataSource=" + dataSource + ", definition=" + new TreeSet<>(definition.keySet()) + "]";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TAG);
            out.writeString(dataSource);
            out.writeGenericValue(definition);
        }

        /**
         * The definition as the request a registration would have made, so one validator serves both arms. The name a
         * registration takes from its URL path is the data source's here, that being the only name an inline read has.
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
                String json = Strings.toString(builder);
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
                    return PutDatasetAction.Request.fromXContent(parser, timeout, timeout, dataSource);
                }
            } catch (IOException e) {
                // JSON over an in-memory map and string; an IOException here is a broken invariant, not input.
                throw new IllegalStateException("could not read an inline dataset definition", e);
            }
        }
    }

    /**
     * Refuses {@code plan} if it carries an arm some node could not read. Decided here rather than at the serializer,
     * where refusing would fail part of a query and leave the rest of it reading.
     *
     * @param minimumVersion the oldest transport version in this cluster
     */
    static void ensureEveryReferenceCanTravel(LogicalPlan plan, TransportVersion minimumVersion) {
        if (minimumVersion.supports(DATASET_REFERENCE)) {
            return;
        }
        plan.forEachDown(node -> {
            if (referenceOf(node) instanceof Inline) {
                throw new VerificationException(NOT_EVERYWHERE);
            }
        });
    }

    /** The reference a leaf carries, or {@code null} for every other plan node. */
    @Nullable
    private static DatasetReference referenceOf(LogicalPlan node) {
        if (node instanceof UnresolvedExternalRelation unresolved) {
            return unresolved.datasetReference();
        }
        if (node instanceof ExternalRelation resolved) {
            return resolved.datasetReference();
        }
        return null;
    }

    /**
     * Reads the slot, in whichever shape the stream has it: a reference, a bare name (which is the {@link Named} arm),
     * or nothing at all on a stream predating the slot.
     */
    @Nullable
    static DatasetReference readOptionalFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(DATASET_REFERENCE)) {
            return in.readOptionalWriteable(DatasetReference::readFrom);
        }
        if (in.getTransportVersion().supports(DATASET_NAME) == false) {
            return null;
        }
        String name = in.readOptionalString();
        return name == null ? null : new Named(name);
    }

    /**
     * Writes the slot in the shape the stream can read: a {@link Named} arm toward an older node as the bare name it
     * has always read. An {@link Inline} arm cannot be written that way and was refused while the query was planned
     * ({@link #ensureEveryReferenceCanTravel}), so reaching here with one is a target older than the coordinator saw.
     */
    static void writeOptional(StreamOutput out, @Nullable DatasetReference reference) throws IOException {
        if (out.getTransportVersion().supports(DATASET_REFERENCE)) {
            out.writeOptionalWriteable(reference);
            return;
        }
        // Before the slot's own version check below, which returns without writing and would drop this silently.
        if (reference instanceof Inline) {
            throw new IllegalArgumentException(NOT_EVERYWHERE);
        }
        if (out.getTransportVersion().supports(DATASET_NAME) == false) {
            return;
        }
        out.writeOptionalString(reference == null ? null : ((Named) reference).name());
    }

    private static DatasetReference readFrom(StreamInput in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case Named.TAG -> new Named(in.readString());
            case Inline.TAG -> {
                String dataSource = in.readString();
                Object definition = in.readGenericValue();
                if (definition instanceof Map<?, ?> fields) {
                    yield new Inline(dataSource, inlineDefinition(fields));
                }
                throw new IOException("a dataset reference's inline definition is not a map");
            }
            default -> throw new IOException("unknown dataset reference tag [" + tag + "]");
        };
    }

    /**
     * Checked rather than cast, so a malformed wire payload is an {@link IOException} like the tag's default arm
     * rather than a {@link ClassCastException} out of deserialization. Top level only, matching {@code Map.copyOf}.
     */
    private static Map<String, Object> inlineDefinition(Map<?, ?> fields) throws IOException {
        Map<String, Object> definition = new LinkedHashMap<>(fields.size());
        for (Map.Entry<?, ?> field : fields.entrySet()) {
            if (field.getKey() instanceof String key) {
                if (field.getValue() == null) {
                    throw new IOException("a dataset reference's inline definition sets [" + key + "] to null");
                }
                definition.put(key, field.getValue());
            } else {
                throw new IOException("a dataset reference's inline definition has a field not named by a string");
            }
        }
        return definition;
    }
}
