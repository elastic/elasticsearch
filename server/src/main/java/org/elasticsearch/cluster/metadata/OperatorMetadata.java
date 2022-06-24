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
import java.util.TreeSet;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metadata class that contains information about cluster settings/entities set
 * in an operator mode.
 *
 * <p>
 * These types of settings are read only through the REST API,
 * and cannot be modified by the end user.
 * </p>
 */
public record OperatorMetadata(
    String namespace,
    Long version,
    Map<String, OperatorHandlerMetadata> handlers,
    OperatorErrorMetadata errorMetadata
) implements SimpleDiffable<OperatorMetadata>, ToXContentFragment {

    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField HANDLERS = new ParseField("handlers");
    private static final ParseField ERRORS_METADATA = new ParseField("errors");

    /**
     * OperatorMetadata contains information about settings set in operator mode.
     *
     * <p>
     * These settings cannot be updated by the end user and are set outside of the
     * REST layer, e.g. through file based settings or by plugin/modules.
     * </p>
     *
     * @param namespace     The namespace of the setting creator, e.g. file_settings, security plugin, etc.
     * @param version       The update version, must increase with each update
     * @param handlers      Per state update handler information on key set in by this update. These keys are validated at REST time.
     * @param errorMetadata If the update failed for some reason, this is where we store the error information metadata.
     */
    public OperatorMetadata {}

    /**
     * Creates a set intersection between cluster state keys set by a given operator handler and the input set.
     *
     * <p>
     * This method is to be used to check if a REST action handler is allowed to modify certain cluster state.
     * </p>
     *
     * @param handlerName the name of the operator handler we need to check for keys
     * @param modified a set of keys we want to see if we can modify.
     * @return
     */
    public Set<String> conflicts(String handlerName, Set<String> modified) {
        OperatorHandlerMetadata handlerMetadata = handlers.get(handlerName);
        if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> intersect = new HashSet<>(handlerMetadata.keys());
        intersect.retainAll(modified);
        return Collections.unmodifiableSet(intersect);
    }

    /**
     * Reads an {@link OperatorMetadata} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link OperatorMetadata}
     * @throws IOException
     */
    public static OperatorMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString()).version(in.readLong());

        int handlersSize = in.readVInt();
        for (int i = 0; i < handlersSize; i++) {
            OperatorHandlerMetadata handler = OperatorHandlerMetadata.readFrom(in);
            builder.putHandler(handler);
        }

        builder.errorMetadata(in.readOptionalWriteable(OperatorErrorMetadata::readFrom));
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
     * Reads an {@link OperatorMetadata} {@link Diff} from {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read the diff from
     * @return a {@link Diff} of {@link OperatorMetadata}
     * @throws IOException
     */
    public static Diff<OperatorMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(OperatorMetadata::readFrom, in);
    }

    /**
     * Empty {@link org.elasticsearch.cluster.DiffableUtils.MapDiff} helper for metadata backwards compatibility.
     */
    public static final DiffableUtils.MapDiff<String, OperatorMetadata, Map<String, OperatorMetadata>> EMPTY_DIFF =
        new DiffableUtils.MapDiff<>(null, null, List.of(), List.of(), List.of()) {
            @Override
            public Map<String, OperatorMetadata> apply(Map<String, OperatorMetadata> part) {
                return part;
            }
        };

    /**
     * Convenience method for creating a {@link Builder} for {@link OperatorMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link OperatorMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace) {
        return new Builder(namespace);
    }

    /**
     * Convenience method for creating a {@link Builder} for {@link OperatorMetadata}
     *
     * @param namespace the namespace under which we'll store the {@link OperatorMetadata}
     * @param metadata an existing {@link OperatorMetadata}
     * @return {@link Builder}
     */
    public static Builder builder(String namespace, OperatorMetadata metadata) {
        return new Builder(namespace, metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(namespace());
        builder.field(VERSION.getPreferredName(), version);
        builder.startObject(HANDLERS.getPreferredName());
        var sortedKeys = new TreeSet<>(handlers.keySet());
        for (var key : sortedKeys) {
            handlers.get(key).toXContent(builder, params);
        }
        builder.endObject();
        builder.field(ERRORS_METADATA.getPreferredName(), errorMetadata);
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<OperatorMetadata, String> PARSER = new ConstructingObjectParser<>(
        "operator_metadata",
        false,
        (a, namespace) -> {
            Map<String, OperatorHandlerMetadata> handlers = new HashMap<>();
            @SuppressWarnings("unchecked")
            List<OperatorHandlerMetadata> handlersList = (List<OperatorHandlerMetadata>) a[1];
            handlersList.forEach(h -> handlers.put(h.name(), h));

            return new OperatorMetadata(namespace, (Long) a[0], Map.copyOf(handlers), (OperatorErrorMetadata) a[2]);
        }
    );

    static {
        PARSER.declareLong(constructorArg(), VERSION);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, name) -> OperatorHandlerMetadata.fromXContent(p, name), HANDLERS);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> OperatorErrorMetadata.fromXContent(p), null, ERRORS_METADATA);
    }

    /**
     * Reads {@link OperatorMetadata} from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @return {@link OperatorMetadata}
     * @throws IOException
     */
    public static OperatorMetadata fromXContent(final XContentParser parser) throws IOException {
        parser.nextToken();
        return PARSER.apply(parser, parser.currentName());
    }

    /**
     * Builder class for {@link OperatorMetadata}
     */
    public static class Builder {
        private final String namespace;
        private Long version;
        private Map<String, OperatorHandlerMetadata> handlers;
        OperatorErrorMetadata errorMetadata;

        /**
         * Empty builder for OperatorMetadata. The operator metadata namespace is a required parameter
         *
         * @param namespace The namespace for this metadata
         */
        public Builder(String namespace) {
            this.namespace = namespace;
            this.version = 0L;
            this.handlers = new HashMap<>();
            this.errorMetadata = null;
        }

        /**
         * Creates an operator metadata builder
         *
         * @param namespace the namespace for which we are storing metadata, e.g. file_settings
         * @param metadata  the previous metadata
         */
        public Builder(String namespace, OperatorMetadata metadata) {
            this(namespace);
            if (metadata != null) {
                this.version = metadata.version;
                this.handlers = new HashMap<>(metadata.handlers);
                this.errorMetadata = metadata.errorMetadata;
            }
        }

        /**
         * Stores the version for the operator metadata.
         *
         * <p>
         * Each new cluster state update in operator
         * mode requires a version bump. The version increase doesn't have to be monotonic.
         * </p>
         * @param version the new operator metadata version
         * @return {@link Builder}
         */
        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        /**
         * Adds {@link OperatorErrorMetadata} if we need to store error information about certain
         * operator state processing.
         *
         * @param errorMetadata {@link OperatorErrorMetadata}
         * @return {@link Builder}
         */
        public Builder errorMetadata(OperatorErrorMetadata errorMetadata) {
            this.errorMetadata = errorMetadata;
            return this;
        }

        /**
         * Adds an {@link OperatorHandlerMetadata} for this {@link OperatorMetadata}.
         *
         * <p>
         * The handler metadata is stored in a map, keyed off the {@link OperatorHandlerMetadata} name. Previously
         * stored {@link OperatorHandlerMetadata} for a given name is overwritten.
         * </p>
         *
         * @param handler {@link OperatorHandlerMetadata}
         * @return {@link Builder}
         */
        public Builder putHandler(OperatorHandlerMetadata handler) {
            this.handlers.put(handler.name(), handler);
            return this;
        }

        /**
         * Builds an {@link OperatorMetadata} from this builder.
         *
         * @return {@link OperatorMetadata}
         */
        public OperatorMetadata build() {
            return new OperatorMetadata(namespace, version, Collections.unmodifiableMap(handlers), errorMetadata);
        }
    }
}
