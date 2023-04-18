/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xcontent.ObjectParser.fromList;

/**
 * A request to add/remove aliases for one or more indices.
 */
public class IndicesAliasesRequest extends AcknowledgedRequest<IndicesAliasesRequest> implements IndicesRequest, ToXContentObject {

    private List<AliasActions> allAliasActions = new ArrayList<>();
    private String origin = "";

    // indices options that require every specified index to exist, expand wildcards only to open
    // indices, don't allow that no indices are resolved from wildcard expressions and resolve the
    // expressions only against indices
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);

    public IndicesAliasesRequest(StreamInput in) throws IOException {
        super(in);
        allAliasActions = in.readList(AliasActions::new);
        origin = in.readOptionalString();
    }

    public IndicesAliasesRequest() {}

    /**
     * Request to take one or more actions on one or more indexes and alias combinations.
     */
    public static class AliasActions implements AliasesRequest, Writeable, ToXContentObject {

        private static final ParseField INDEX = new ParseField("index");
        private static final ParseField INDICES = new ParseField("indices");
        private static final ParseField ALIAS = new ParseField("alias");
        private static final ParseField ALIASES = new ParseField("aliases");
        private static final ParseField FILTER = new ParseField("filter");
        private static final ParseField ROUTING = new ParseField("routing");
        private static final ParseField INDEX_ROUTING = new ParseField("index_routing", "indexRouting", "index-routing");
        private static final ParseField SEARCH_ROUTING = new ParseField("search_routing", "searchRouting", "search-routing");
        private static final ParseField IS_WRITE_INDEX = new ParseField("is_write_index");
        private static final ParseField IS_HIDDEN = new ParseField("is_hidden");
        private static final ParseField MUST_EXIST = new ParseField("must_exist");

        private static final ParseField ADD = new ParseField("add");
        private static final ParseField REMOVE = new ParseField("remove");
        private static final ParseField REMOVE_INDEX = new ParseField("remove_index");

        public enum Type {
            ADD((byte) 0, AliasActions.ADD),
            REMOVE((byte) 1, AliasActions.REMOVE),
            REMOVE_INDEX((byte) 2, AliasActions.REMOVE_INDEX);

            private final byte value;
            private final String fieldName;

            Type(byte value, ParseField field) {
                this.value = value;
                this.fieldName = field.getPreferredName();
            }

            public byte value() {
                return value;
            }

            public static Type fromValue(byte value) {
                return switch (value) {
                    case 0 -> ADD;
                    case 1 -> REMOVE;
                    case 2 -> REMOVE_INDEX;
                    default -> throw new IllegalArgumentException("No type for action [" + value + "]");
                };
            }
        }

        /**
         * Build a new {@link AliasAction} to add aliases.
         */
        public static AliasActions add() {
            return new AliasActions(AliasActions.Type.ADD);
        }

        /**
         * Build a new {@link AliasAction} to remove aliases.
         */
        public static AliasActions remove() {
            return new AliasActions(AliasActions.Type.REMOVE);
        }

        /**
         * Build a new {@link AliasAction} to remove an index.
         */
        public static AliasActions removeIndex() {
            return new AliasActions(AliasActions.Type.REMOVE_INDEX);
        }

        private static ObjectParser<AliasActions, Void> parser(String name, Supplier<AliasActions> supplier) {
            ObjectParser<AliasActions, Void> parser = new ObjectParser<>(name, supplier);
            parser.declareString((action, index) -> {
                if (action.indices() != null) {
                    throw new IllegalArgumentException("Only one of [index] and [indices] is supported");
                }
                action.index(index);
            }, INDEX);
            parser.declareStringArray(fromList(String.class, (action, indices) -> {
                if (action.indices() != null) {
                    throw new IllegalArgumentException("Only one of [index] and [indices] is supported");
                }
                action.indices(indices);
            }), INDICES);
            parser.declareString((action, alias) -> {
                if (action.aliases() != null && action.aliases().length != 0) {
                    throw new IllegalArgumentException("Only one of [alias] and [aliases] is supported");
                }
                action.alias(alias);
            }, ALIAS);
            parser.declareStringArray(fromList(String.class, (action, aliases) -> {
                if (action.aliases() != null && action.aliases().length != 0) {
                    throw new IllegalArgumentException("Only one of [alias] and [aliases] is supported");
                }
                action.aliases(aliases);
            }), ALIASES);
            return parser;
        }

        private static final ObjectParser<AliasActions, Void> ADD_PARSER = parser(ADD.getPreferredName(), AliasActions::add);
        private static final ObjectParser<AliasActions, Void> REMOVE_PARSER = parser(REMOVE.getPreferredName(), AliasActions::remove);
        private static final ObjectParser<AliasActions, Void> REMOVE_INDEX_PARSER = parser(
            REMOVE_INDEX.getPreferredName(),
            AliasActions::removeIndex
        );
        static {
            ADD_PARSER.declareObject(AliasActions::filter, (parser, m) -> {
                try {
                    return parser.mapOrdered();
                } catch (IOException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Problems parsing [filter]", e);
                }
            }, FILTER);
            // Since we need to support numbers AND strings here we have to use ValueType.INT.
            ADD_PARSER.declareField(AliasActions::routing, XContentParser::text, ROUTING, ValueType.INT);
            ADD_PARSER.declareField(AliasActions::indexRouting, XContentParser::text, INDEX_ROUTING, ValueType.INT);
            ADD_PARSER.declareField(AliasActions::searchRouting, XContentParser::text, SEARCH_ROUTING, ValueType.INT);
            ADD_PARSER.declareField(AliasActions::writeIndex, XContentParser::booleanValue, IS_WRITE_INDEX, ValueType.BOOLEAN);
            ADD_PARSER.declareField(AliasActions::isHidden, XContentParser::booleanValue, IS_HIDDEN, ValueType.BOOLEAN);
            REMOVE_PARSER.declareField(AliasActions::mustExist, XContentParser::booleanValue, MUST_EXIST, ValueType.BOOLEAN);
        }

        /**
         * Parser for any one {@link AliasAction}.
         */
        public static final ConstructingObjectParser<AliasActions, Void> PARSER = new ConstructingObjectParser<>("alias_action", a -> {
            // Take the first action and complain if there are more than one actions
            AliasActions action = null;
            for (Object o : a) {
                if (o != null) {
                    if (action == null) {
                        action = (AliasActions) o;
                    } else {
                        throw new IllegalArgumentException("Too many operations declared on operation entry");
                    }
                }
            }
            return action;
        });
        static {
            PARSER.declareObject(optionalConstructorArg(), ADD_PARSER, ADD);
            PARSER.declareObject(optionalConstructorArg(), REMOVE_PARSER, REMOVE);
            PARSER.declareObject(optionalConstructorArg(), REMOVE_INDEX_PARSER, REMOVE_INDEX);
        }

        private final AliasActions.Type type;
        private String[] indices;
        private String[] aliases = Strings.EMPTY_ARRAY;
        private String[] originalAliases = Strings.EMPTY_ARRAY;
        private String filter;
        private String routing;
        private String indexRouting;
        private String searchRouting;
        private Boolean writeIndex;
        private Boolean isHidden;
        private Boolean mustExist;

        public AliasActions(AliasActions.Type type) {
            this.type = type;
        }

        /**
         * Read from a stream.
         */
        public AliasActions(StreamInput in) throws IOException {
            type = AliasActions.Type.fromValue(in.readByte());
            indices = in.readStringArray();
            aliases = in.readStringArray();
            filter = in.readOptionalString();
            routing = in.readOptionalString();
            searchRouting = in.readOptionalString();
            indexRouting = in.readOptionalString();
            writeIndex = in.readOptionalBoolean();
            isHidden = in.readOptionalBoolean();
            originalAliases = in.readStringArray();
            mustExist = in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(type.value());
            out.writeStringArray(indices);
            out.writeStringArray(aliases);
            out.writeOptionalString(filter);
            out.writeOptionalString(routing);
            out.writeOptionalString(searchRouting);
            out.writeOptionalString(indexRouting);
            out.writeOptionalBoolean(writeIndex);
            out.writeOptionalBoolean(isHidden);
            out.writeStringArray(originalAliases);
            out.writeOptionalBoolean(mustExist);
        }

        /**
         * Validate that the action is sane. Called when the action is added to the request because actions can be invalid while being
         * built.
         */
        void validate() {
            if (indices == null) {
                throw new IllegalArgumentException("One of [index] or [indices] is required");
            }
            if (type != AliasActions.Type.REMOVE_INDEX && (aliases == null || aliases.length == 0)) {
                throw new IllegalArgumentException("One of [alias] or [aliases] is required");
            }
        }

        /**
         * Type of the action to perform.
         */
        public AliasActions.Type actionType() {
            return type;
        }

        @Override
        public AliasActions indices(String... indices) {
            if (indices == null || indices.length == 0) {
                throw new IllegalArgumentException("[indices] can't be empty");
            }
            for (String index : indices) {
                if (false == Strings.hasLength(index)) {
                    throw new IllegalArgumentException("[indices] can't contain empty string");
                }
            }
            this.indices = indices;
            return this;
        }

        /**
         * Set the index this action is operating on.
         */
        public AliasActions index(String index) {
            if (false == Strings.hasLength(index)) {
                throw new IllegalArgumentException("[index] can't be empty string");
            }
            this.indices = new String[] { index };
            return this;
        }

        /**
         * Aliases to use with this action.
         */
        public AliasActions aliases(String... aliases) {
            if (type == AliasActions.Type.REMOVE_INDEX) {
                throw new IllegalArgumentException("[aliases] is unsupported for [" + type + "]");
            }
            if (aliases == null || aliases.length == 0) {
                throw new IllegalArgumentException("[aliases] can't be empty");
            }
            for (String alias : aliases) {
                if (false == Strings.hasLength(alias)) {
                    throw new IllegalArgumentException("[aliases] can't contain empty string");
                }
            }
            this.aliases = aliases;
            this.originalAliases = aliases;
            return this;
        }

        /**
         * Set the alias this action is operating on.
         */
        public AliasActions alias(String alias) {
            if (type == AliasActions.Type.REMOVE_INDEX) {
                throw new IllegalArgumentException("[alias] is unsupported for [" + type + "]");
            }
            if (false == Strings.hasLength(alias)) {
                throw new IllegalArgumentException("[alias] can't be empty string");
            }
            this.aliases = new String[] { alias };
            this.originalAliases = aliases;
            return this;
        }

        /**
         * Set the default routing.
         */
        public AliasActions routing(String routing) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[routing] is unsupported for [" + type + "]");
            }
            this.routing = routing;
            return this;
        }

        public String searchRouting() {
            return searchRouting == null ? routing : searchRouting;
        }

        public AliasActions searchRouting(String searchRouting) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[search_routing] is unsupported for [" + type + "]");
            }
            this.searchRouting = searchRouting;
            return this;
        }

        public String routing() {
            return routing;
        }

        public String indexRouting() {
            return indexRouting == null ? routing : indexRouting;
        }

        public AliasActions indexRouting(String indexRouting) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[index_routing] is unsupported for [" + type + "]");
            }
            this.indexRouting = indexRouting;
            return this;
        }

        public String filter() {
            return filter;
        }

        public AliasActions filter(String filter) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[filter] is unsupported for [" + type + "]");
            }
            this.filter = filter;
            return this;
        }

        public AliasActions filter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                this.filter = null;
                return this;
            }
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(filter);
                this.filter = Strings.toString(builder);
                return this;
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + filter + "]", e);
            }
        }

        public AliasActions filter(QueryBuilder filter) {
            if (filter == null) {
                this.filter = null;
                return this;
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                filter.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
                this.filter = Strings.toString(builder);
                return this;
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public AliasActions writeIndex(Boolean writeIndex) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[is_write_index] is unsupported for [" + type + "]");
            }
            this.writeIndex = writeIndex;
            return this;
        }

        public Boolean writeIndex() {
            return writeIndex;
        }

        public AliasActions isHidden(Boolean isHidden) {
            if (type != AliasActions.Type.ADD) {
                throw new IllegalArgumentException("[" + IS_HIDDEN.getPreferredName() + "] is unsupported for [" + type + "]");
            }
            this.isHidden = isHidden;
            return this;
        }

        public Boolean isHidden() {
            return isHidden;
        }

        public AliasActions mustExist(Boolean mustExist) {
            if (type != Type.REMOVE) {
                throw new IllegalArgumentException("[" + MUST_EXIST.getPreferredName() + "] is unsupported for [" + type + "]");
            }
            this.mustExist = mustExist;
            return this;
        }

        public Boolean mustExist() {
            return mustExist;
        }

        @Override
        public String[] aliases() {
            return aliases;
        }

        @Override
        public void replaceAliases(String... aliases) {
            this.aliases = aliases;
        }

        @Override
        public String[] getOriginalAliases() {
            return originalAliases;
        }

        @Override
        public boolean expandAliasesWildcards() {
            // remove operations support wildcards among aliases, add operations don't
            return type == Type.REMOVE;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(type.fieldName);
            if (null != indices && 0 != indices.length) {
                builder.array(INDICES.getPreferredName(), indices);
            }
            if (0 != aliases.length) {
                builder.array(ALIASES.getPreferredName(), aliases);
            }
            if (false == Strings.isEmpty(filter)) {
                try (InputStream stream = new BytesArray(filter).streamInput()) {
                    builder.rawField(FILTER.getPreferredName(), stream, XContentType.JSON);
                }
            }
            if (false == Strings.isEmpty(routing)) {
                builder.field(ROUTING.getPreferredName(), routing);
            }
            if (false == Strings.isEmpty(indexRouting)) {
                builder.field(INDEX_ROUTING.getPreferredName(), indexRouting);
            }
            if (false == Strings.isEmpty(searchRouting)) {
                builder.field(SEARCH_ROUTING.getPreferredName(), searchRouting);
            }
            if (null != writeIndex) {
                builder.field(IS_WRITE_INDEX.getPreferredName(), writeIndex);
            }
            if (null != isHidden) {
                builder.field(IS_HIDDEN.getPreferredName(), isHidden);
            }
            if (null != mustExist) {
                builder.field(MUST_EXIST.getPreferredName(), mustExist);
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }

        public static AliasActions fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        @Override
        public String toString() {
            return "AliasActions["
                + "type="
                + type
                + ",indices="
                + Arrays.toString(indices)
                + ",aliases="
                + Arrays.deepToString(aliases)
                + ",filter="
                + filter
                + ",routing="
                + routing
                + ",indexRouting="
                + indexRouting
                + ",searchRouting="
                + searchRouting
                + ",writeIndex="
                + writeIndex
                + ",isHidden="
                + isHidden
                + ",mustExist="
                + mustExist
                + "]";
        }

        // equals, and hashCode implemented for easy testing of round trip
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            AliasActions other = (AliasActions) obj;
            return Objects.equals(type, other.type)
                && Arrays.equals(indices, other.indices)
                && Arrays.equals(aliases, other.aliases)
                && Objects.equals(filter, other.filter)
                && Objects.equals(routing, other.routing)
                && Objects.equals(indexRouting, other.indexRouting)
                && Objects.equals(searchRouting, other.searchRouting)
                && Objects.equals(writeIndex, other.writeIndex)
                && Objects.equals(isHidden, other.isHidden)
                && Objects.equals(mustExist, other.mustExist);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, indices, aliases, filter, routing, indexRouting, searchRouting, writeIndex, isHidden, mustExist);
        }
    }

    public String origin() {
        return origin;
    }

    public IndicesAliasesRequest origin(final String origin) {
        this.origin = Objects.requireNonNull(origin);
        return this;
    }

    /**
     * Add the action to this request and validate it.
     */
    public IndicesAliasesRequest addAliasAction(AliasActions aliasAction) {
        aliasAction.validate();
        allAliasActions.add(aliasAction);
        return this;
    }

    List<AliasActions> aliasActions() {
        return this.allAliasActions;
    }

    public List<AliasActions> getAliasActions() {
        return aliasActions();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (allAliasActions.isEmpty()) {
            return addValidationError("Must specify at least one alias action", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(allAliasActions);
        out.writeOptionalString(origin);
    }

    public IndicesOptions indicesOptions() {
        return INDICES_OPTIONS;
    }

    @Override
    public String[] indices() {
        return allAliasActions.stream().flatMap(aliasActions -> Arrays.stream(aliasActions.indices())).toArray(String[]::new);
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("actions");
        for (AliasActions action : allAliasActions) {
            action.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static final ObjectParser<IndicesAliasesRequest, Void> PARSER = new ObjectParser<>("aliases", IndicesAliasesRequest::new);
    static {
        PARSER.declareObjectArray((request, actions) -> {
            for (AliasActions action : actions) {
                request.addAliasAction(action);
            }
        }, AliasActions.PARSER, new ParseField("actions"));
    }

    public static IndicesAliasesRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
