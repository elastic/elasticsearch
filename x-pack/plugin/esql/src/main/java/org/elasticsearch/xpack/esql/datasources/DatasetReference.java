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

/**
 * Which dataset an external source reads: one slot, either a registered dataset's {@link Named name} or an
 * {@link Inline} definition the query supplied. The reading node resolves whichever arrived.
 * <p>
 * <b>One slot rather than two.</b> A name and a definition are interchangeable inputs to the same read, so they
 * occupy one field and {@link DatasetConfigResolver#rebind} has one input to resolve. The shape it replaced — a
 * definition sitting beside a nullable name, resolved by testing the name for null and then testing the other field —
 * is one step from asking where a relation came from, and a later syntax that embeds a definition (a view, say) would
 * have had to add a third test. Here it lands on the {@code Inline} arm and nothing branches.
 * <p>
 * <b>Why a definition travels at all.</b> A data node does not read a source under the configuration the plan arrived
 * with; it rebuilds that configuration from its own cluster state, because a plan is not trusted to have been built by
 * a coordinator the caller was allowed to use. Rebuilding needs the whole definition rather than a name:
 * {@link DatasetRewriter#datasetConfig} takes a {@code Dataset} and the {@code DataSource} behind it, and the settings
 * it produces depend on the declared mapping ({@link PartitionSpec#alignWithMapping}).
 * <p>
 * <b>What does not travel.</b> The {@code Inline} arm carries a data source <i>name</i>, never its settings: the
 * definition says what to read and the name says whose credentials to read it with, and those credentials are looked
 * up in the receiving node's own registry. A dataset setting that shadows one of the data source's secrets is refused
 * by the one validator both arms go through, so the arm cannot smuggle one either.
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TAG);
            out.writeString(dataSource);
            out.writeGenericValue(definition);
        }

        /**
         * The definition as the request a registration would have made, so that one validator serves both arms and
         * the two cannot come to disagree about what a definition may say. The name a registration takes from its URL
         * path is the data source's here: it is the only name an inline read has, and it is the name the read is
         * authorized under.
         * <p>
         * A definition naming its own data source is refused with an {@link IllegalArgumentException}; anything else a
         * registration body cannot say is refused by the registration's own parser, in the registration's own words.
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
     * Refuses {@code plan} if it carries an arm some node in the cluster could not read, which is decided here rather
     * than where the plan is serialized: a leaf is read on whichever nodes hold its files, so a refusal at the
     * serializer would fail part of a query and leave the rest of it reading, and which nodes those are is not known
     * while the plan is still being analyzed. Walks both leaf shapes a table function may return, resolved or not.
     *
     * @param minimumVersion the oldest transport version in this cluster, so a plan that passes has no target it
     *                       cannot be sent to
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
     * Writes the slot in the shape the stream can read. A {@link Named} arm toward a node predating the reference is
     * written as the bare name that node has always read, byte for byte, so every query that works today keeps
     * working. An {@link Inline} arm cannot be written that way and was refused while the query was planned
     * ({@link #ensureEveryReferenceCanTravel}); reaching here with one means a target older than the minimum the
     * coordinator saw, and dropping it would leave that node with nothing to resolve and no way to know.
     */
    static void writeOptional(StreamOutput out, @Nullable DatasetReference reference) throws IOException {
        if (out.getTransportVersion().supports(DATASET_REFERENCE)) {
            out.writeOptionalWriteable(reference);
            return;
        }
        if (out.getTransportVersion().supports(DATASET_NAME) == false) {
            return;
        }
        if (reference instanceof Inline) {
            throw new IllegalArgumentException(NOT_EVERYWHERE);
        }
        out.writeOptionalString(reference == null ? null : ((Named) reference).name());
    }

    private static DatasetReference readFrom(StreamInput in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case Named.TAG -> new Named(in.readString());
            case Inline.TAG -> {
                String dataSource = in.readString();
                @SuppressWarnings("unchecked")
                Map<String, Object> definition = (Map<String, Object>) in.readGenericValue();
                yield new Inline(dataSource, definition);
            }
            default -> throw new IOException("unknown dataset reference tag [" + tag + "]");
        };
    }
}
