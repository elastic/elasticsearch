/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
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
 * Metadata class that contains information about immutable cluster state set
 * through file based settings or by modules/plugins.
 *
 * <p>
 * These types of cluster settings/entities can be read through the REST API,
 * but can only be modified through a versioned 'operator mode' update, e.g.
 * file based settings or module/plugin upgrade.
 */
public record ImmutableStateMetadata(
    String namespace,
    Long version,
    Map<String, ImmutableStateHandlerMetadata> handlers,
    ImmutableStateErrorMetadata errorMetadata
) implements SimpleDiffable<ImmutableStateMetadata>, ToXContentFragment {

    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField HANDLERS = new ParseField("handlers");
    private static final ParseField ERRORS_METADATA = new ParseField("errors");

    /**
     * ImmutableStateMetadata contains information about immutable cluster settings.
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
    public ImmutableStateMetadata {}

    /**
     * Creates a set intersection between cluster state keys set by a given {@link ImmutableClusterStateHandler}
     * and the input set.
     *
     * <p>
     * This method is to be used to check if a REST action handler is allowed to modify certain cluster state.
     *
     * @param handlerName the name of the immutable state handler we need to check for keys
     * @param modified a set of keys we want to see if we can modify.
     * @return
     */
    public Set<String> conflicts(String handlerName, Set<String> modified) {
        ImmutableStateHandlerMetadata handlerMetadata = handlers.get(handlerName);
        if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> intersect = new HashSet<>(handlerMetadata.keys());
        intersect.retainAll(modified);
        return Collections.unmodifiableSet(intersect);
    }

    /**
     * Reads an {@link ImmutableStateMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ImmutableStateMetadata}
     * @throws IOException
     */
    public static ImmutableStateMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString()).version(in.readLong());

        int handlersSize = in.readVInt();
        for (int i = 0; i < handlersSize; i++) {
            ImmutableStateHandlerMetadata handler = ImmutableStateHandlerMetadata.readFrom(in);
            builder.putHandler(handler);
        }

        builder.errorMetadata(in.readOptionalWriteable(ImmutableStateErrorMetadata::readFrom));
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
     * Reads an {@link ImmutableStateMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link ImmutableStateMetadata}
     * @throws IOException
     */
    public static Diff<ImmutableStateMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ImmutableStateMetadata::readFrom, in);
    }

    /**
     * Empty {@link org.elasticsearch.cluster.DiffableUtils.MapDiff} helper for metadata backwards compatibility.
     */
    public static final DiffableUtils.MapDiff<String, ImmutableStateMetadata, Map<String, ImmutableStateMetadata>> EMPTY_DIFF =
        new DiffableUtils.MapDiff<>(null, null, List.of(), List.of(), List.of()) {
            @Override
            public Map<String, ImmutableStateMetadata> apply(Map<String, ImmutableStateMetadata> part) {
                return part;
            }
        };

    /**
     * Convenience method for creating a {@link Builder} for {@link ImmutableStateMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link ImmutableStateMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace) {
        return new Builder(namespace);
    }

    /**
     * Convenience method for creating a {@link Builder} for {@link ImmutableStateMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link ImmutableStateMetadata}
     * @param metadata an existing {@link ImmutableStateMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace, ImmutableStateMetadata metadata) {
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

    private static final ConstructingObjectParser<ImmutableStateMetadata, String> PARSER = new ConstructingObjectParser<>(
        "immutable_state_metadata",
        false,
        (a, namespace) -> {
            Map<String, ImmutableStateHandlerMetadata> handlers = new HashMap<>();
            @SuppressWarnings("unchecked")
            List<ImmutableStateHandlerMetadata> handlersList = (List<ImmutableStateHandlerMetadata>) a[1];
            handlersList.forEach(h -> handlers.put(h.name(), h));

            return new ImmutableStateMetadata(namespace, (Long) a[0], Map.copyOf(handlers), (ImmutableStateErrorMetadata) a[2]);
        }
    );

    static {
        PARSER.declareLong(constructorArg(), VERSION);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, name) -> ImmutableStateHandlerMetadata.fromXContent(p, name), HANDLERS);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> ImmutableStateErrorMetadata.fromXContent(p), null, ERRORS_METADATA);
    }

    /**
     * Reads {@link ImmutableStateMetadata} from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @return {@link ImmutableStateMetadata}
     * @throws IOException
     */
    public static ImmutableStateMetadata fromXContent(final XContentParser parser) throws IOException {
        parser.nextToken();
        return PARSER.apply(parser, parser.currentName());
    }

    /**
     * Builder class for {@link ImmutableStateMetadata}
     */
    public static class Builder {
        private final String namespace;
        private Long version;
        private Map<String, ImmutableStateHandlerMetadata> handlers;
        ImmutableStateErrorMetadata errorMetadata;

        /**
         * Empty builder for ImmutableStateMetadata.
         * <p>
         * The immutable metadata namespace is a required parameter
         *
         * @param namespace The namespace for this immutable metadata
         */
        public Builder(String namespace) {
            this.namespace = namespace;
            this.version = 0L;
            this.handlers = new HashMap<>();
            this.errorMetadata = null;
        }

        /**
         * Creates an immutable state metadata builder
         *
         * @param namespace the namespace for which we are storing metadata, e.g. file_settings
         * @param metadata  the previous metadata
         */
        public Builder(String namespace, ImmutableStateMetadata metadata) {
            this(namespace);
            if (metadata != null) {
                this.version = metadata.version;
                this.handlers = new HashMap<>(metadata.handlers);
                this.errorMetadata = metadata.errorMetadata;
            }
        }

        /**
         * Stores the version for the immutable state metadata.
         *
         * <p>
         * Each new immutable cluster state update mode requires a version bump.
         * The version increase doesn't have to be monotonic.
         *
         * @param version the new immutable state metadata version
         * @return {@link Builder}
         */
        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        /**
         * Adds {@link ImmutableStateErrorMetadata} if we need to store error information about certain
         * immutable state processing.
         *
         * @param errorMetadata {@link ImmutableStateErrorMetadata}
         * @return {@link Builder}
         */
        public Builder errorMetadata(ImmutableStateErrorMetadata errorMetadata) {
            this.errorMetadata = errorMetadata;
            return this;
        }

        /**
         * Adds an {@link ImmutableStateHandlerMetadata} for this {@link ImmutableStateMetadata}.
         *
         * <p>
         * The handler metadata is stored in a map, keyed off the {@link ImmutableStateHandlerMetadata} name. Previously
         * stored {@link ImmutableStateHandlerMetadata} for a given name is overwritten.
         *
         * @param handler {@link ImmutableStateHandlerMetadata}
         * @return {@link Builder}
         */
        public Builder putHandler(ImmutableStateHandlerMetadata handler) {
            this.handlers.put(handler.name(), handler);
            return this;
        }

        /**
         * Builds an {@link ImmutableStateMetadata} from this builder.
         *
         * @return {@link ImmutableStateMetadata}
         */
        public ImmutableStateMetadata build() {
            return new ImmutableStateMetadata(namespace, version, Collections.unmodifiableMap(handlers), errorMetadata);
        }
    }
}
