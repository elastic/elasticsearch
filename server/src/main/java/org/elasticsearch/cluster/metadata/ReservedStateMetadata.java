/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metadata class that contains information about reserved cluster state set
 * through file based settings or by modules/plugins.
 *
 * <p>
 * These types of cluster settings/entities can be read through the REST API,
 * but can only be modified through a versioned 'operator mode' update, e.g.
 * file based settings or module/plugin upgrade.
 */
public record ReservedStateMetadata(
    String namespace,
    Long version,
    Map<String, ReservedStateHandlerMetadata> handlers,
    ReservedStateErrorMetadata errorMetadata
) implements SimpleDiffable<ReservedStateMetadata>, ToXContentFragment {

    public static final Long NO_VERSION = Long.MIN_VALUE; // use min long as sentinel for uninitialized version
    public static final Long EMPTY_VERSION = -1L; // use -1 as sentinel for empty metadata
    public static final Long RESTORED_VERSION = 0L; // use 0 as sentinel for metadata restored from snapshot

    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField HANDLERS = new ParseField("handlers");
    private static final ParseField ERRORS_METADATA = new ParseField("errors");

    /**
     * ReservedStateMetadata contains information about reserved cluster settings.
     *
     * <p>
     * These settings cannot be updated by the end user and are set outside of the
     * REST layer, e.g. through file based settings or by plugin/modules.
     *
     * @param namespace     The namespace of the setting creator, e.g. file_settings, security plugin, etc.
     * @param version       The update version, must increase with each update
     * @param handlers      Per state update handler information on key set in by this update. These keys are validated at REST time.
     * @param errorMetadata If the update failed for some reason, this is where we store the error information metadata.
     */
    public ReservedStateMetadata {}

    /**
     * Creates a set intersection between cluster state keys set by a given {@link ReservedClusterStateHandler}
     * and the input set.
     *
     * <p>
     * This method is to be used to check if a REST action handler is allowed to modify certain cluster state.
     *
     * @param handlerName the name of the reserved state handler we need to check for keys
     * @param modified a set of keys we want to see if we can modify.
     * @return
     */
    public Set<String> conflicts(String handlerName, Set<String> modified) {
        ReservedStateHandlerMetadata handlerMetadata = handlers.get(handlerName);
        if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> intersect = new HashSet<>(handlerMetadata.keys());
        intersect.retainAll(modified);
        return Collections.unmodifiableSet(intersect);
    }

    /**
     * Get the reserved keys for the handler name
     *
     * @param handlerName handler name to get keys for
     * @return set of keys for that handler
     */
    public Set<String> keys(String handlerName) {
        ReservedStateHandlerMetadata handlerMetadata = handlers.get(handlerName);
        if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
            return Collections.emptySet();
        }

        return Collections.unmodifiableSet(handlerMetadata.keys());
    }

    /**
     * Reads an {@link ReservedStateMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ReservedStateMetadata}
     * @throws IOException
     */
    public static ReservedStateMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString()).version(in.readLong());

        int handlersSize = in.readVInt();
        for (int i = 0; i < handlersSize; i++) {
            ReservedStateHandlerMetadata handler = ReservedStateHandlerMetadata.readFrom(in);
            builder.putHandler(handler);
        }

        builder.errorMetadata(in.readOptionalWriteable(ReservedStateErrorMetadata::readFrom));
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(namespace);
        out.writeLong(version);
        out.writeCollection(handlers.values());
        out.writeOptionalWriteable(errorMetadata);
    }

    /**
     * Reads an {@link ReservedStateMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link ReservedStateMetadata}
     * @throws IOException
     */
    public static Diff<ReservedStateMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ReservedStateMetadata::readFrom, in);
    }

    /**
     * Convenience method for creating a {@link Builder} for {@link ReservedStateMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link ReservedStateMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace) {
        return new Builder(namespace);
    }

    /**
     * Convenience method for creating a {@link Builder} for {@link ReservedStateMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link ReservedStateMetadata}
     * @param metadata an existing {@link ReservedStateMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace, ReservedStateMetadata metadata) {
        return new Builder(namespace, metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(namespace());
        builder.field(VERSION.getPreferredName(), version);
        builder.startObject(HANDLERS.getPreferredName());
        for (var i = handlers.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator(); i.hasNext();) {
            i.next().getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.field(ERRORS_METADATA.getPreferredName(), errorMetadata);
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<ReservedStateMetadata, String> PARSER = new ConstructingObjectParser<>(
        "reserved_state_metadata",
        false,
        (a, namespace) -> {
            Map<String, ReservedStateHandlerMetadata> handlers = new HashMap<>();
            @SuppressWarnings("unchecked")
            List<ReservedStateHandlerMetadata> handlersList = (List<ReservedStateHandlerMetadata>) a[1];
            handlersList.forEach(h -> handlers.put(h.name(), h));

            return new ReservedStateMetadata(namespace, (Long) a[0], Map.copyOf(handlers), (ReservedStateErrorMetadata) a[2]);
        }
    );

    static {
        PARSER.declareLong(constructorArg(), VERSION);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, name) -> ReservedStateHandlerMetadata.fromXContent(p, name), HANDLERS);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> ReservedStateErrorMetadata.fromXContent(p), null, ERRORS_METADATA);
    }

    /**
     * Reads {@link ReservedStateMetadata} from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @return {@link ReservedStateMetadata}
     * @throws IOException
     */
    public static ReservedStateMetadata fromXContent(final XContentParser parser) throws IOException {
        parser.nextToken();
        return PARSER.apply(parser, parser.currentName());
    }

    /**
     * Builder class for {@link ReservedStateMetadata}
     */
    public static class Builder {
        private final String namespace;
        private Long version;
        private Map<String, ReservedStateHandlerMetadata> handlers;
        ReservedStateErrorMetadata errorMetadata;

        /**
         * Empty builder for ReservedStateMetadata.
         * <p>
         * The reserved metadata namespace is a required parameter
         *
         * @param namespace The namespace for this reserved metadata
         */
        public Builder(String namespace) {
            this.namespace = namespace;
            this.version = NO_VERSION;
            this.handlers = new HashMap<>();
            this.errorMetadata = null;
        }

        /**
         * Creates an reserved state metadata builder
         *
         * @param metadata  the previous metadata
         */
        public Builder(ReservedStateMetadata metadata) {
            this(metadata.namespace);
            this.version = metadata.version;
            this.handlers = new HashMap<>(metadata.handlers);
            this.errorMetadata = metadata.errorMetadata;
        }

        /**
         * Creates an reserved state metadata builder
         *
         * @param namespace the namespace for which we are storing metadata, e.g. file_settings
         * @param metadata  the previous metadata
         */
        public Builder(String namespace, ReservedStateMetadata metadata) {
            this(namespace);
            if (metadata != null) {
                this.version = metadata.version;
                this.handlers = new HashMap<>(metadata.handlers);
                this.errorMetadata = metadata.errorMetadata;
            }
        }

        /**
         * Stores the version for the reserved state metadata.
         *
         * <p>
         * Each new reserved cluster state update mode requires a version bump.
         * The version increase doesn't have to be monotonic.
         *
         * @param version the new reserved state metadata version
         * @return {@link Builder}
         */
        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        /**
         * Adds {@link ReservedStateErrorMetadata} if we need to store error information about certain
         * reserved state processing.
         *
         * @param errorMetadata {@link ReservedStateErrorMetadata}
         * @return {@link Builder}
         */
        public Builder errorMetadata(ReservedStateErrorMetadata errorMetadata) {
            this.errorMetadata = errorMetadata;
            return this;
        }

        /**
         * Adds an {@link ReservedStateHandlerMetadata} for this {@link ReservedStateMetadata}.
         *
         * <p>
         * The handler metadata is stored in a map, keyed off the {@link ReservedStateHandlerMetadata} name. Previously
         * stored {@link ReservedStateHandlerMetadata} for a given name is overwritten.
         *
         * @param handler {@link ReservedStateHandlerMetadata}
         * @return {@link Builder}
         */
        public Builder putHandler(ReservedStateHandlerMetadata handler) {
            this.handlers.put(handler.name(), handler);
            return this;
        }

        /**
         * Returns the current handler metadata stored in the builder
         */
        public ReservedStateHandlerMetadata getHandler(String handlerName) {
            return this.handlers.get(handlerName);
        }

        /**
         * Builds an {@link ReservedStateMetadata} from this builder.
         *
         * @return {@link ReservedStateMetadata}
         */
        public ReservedStateMetadata build() {
            return new ReservedStateMetadata(namespace, version, Collections.unmodifiableMap(handlers), errorMetadata);
        }
    }
}
