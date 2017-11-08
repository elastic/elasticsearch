/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.ObjectParser.fromList;

/**
 * A request to add/remove aliases for one or more indices.
 */
public class IndicesAliasesRequest extends AcknowledgedRequest<IndicesAliasesRequest> {
    private List<AliasActions> allAliasActions = new ArrayList<>();

    // indices options that require every specified index to exist, expand wildcards only to open
    // indices, don't allow that no indices are resolved from wildcard expressions and resolve the
    // expressions only against indices
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, false, true, false, true);

    public IndicesAliasesRequest() {

    }

    /**
     * Request to take one or more actions on one or more indexes and alias combinations.
     */
    public static class AliasActions implements AliasesRequest, Writeable {
        public enum Type {
            ADD((byte) 0),
            REMOVE((byte) 1),
            REMOVE_INDEX((byte) 2);

            private final byte value;

            Type(byte value) {
                this.value = value;
            }

            public byte value() {
                return value;
            }

            public static Type fromValue(byte value) {
                switch (value) {
                    case 0: return ADD;
                    case 1: return REMOVE;
                    case 2: return REMOVE_INDEX;
                    default: throw new IllegalArgumentException("No type for action [" + value + "]");
                }
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
            }, new ParseField("index"));
            parser.declareStringArray(fromList(String.class, (action, indices) -> {
                if (action.indices() != null) {
                    throw new IllegalArgumentException("Only one of [index] and [indices] is supported");
                }
                action.indices(indices);
            }), new ParseField("indices"));
            parser.declareString((action, alias) -> {
                if (action.aliases() != null && action.aliases().length != 0) {
                    throw new IllegalArgumentException("Only one of [alias] and [aliases] is supported");
                }
                action.alias(alias);
            }, new ParseField("alias"));
            parser.declareStringArray(fromList(String.class, (action, aliases) -> {
                if (action.aliases() != null && action.aliases().length != 0) {
                    throw new IllegalArgumentException("Only one of [alias] and [aliases] is supported");
                }
                action.aliases(aliases);
            }), new ParseField("aliases"));
            return parser;
        }

        private static final ObjectParser<AliasActions, Void> ADD_PARSER = parser("add", AliasActions::add);
        static {
            ADD_PARSER.declareObject(AliasActions::filter, (parser, m) -> {
                try {
                    return parser.mapOrdered();
                } catch (IOException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Problems parsing [filter]", e);
                }
            }, new ParseField("filter"));
            // Since we need to support numbers AND strings here we have to use ValueType.INT.
            ADD_PARSER.declareField(AliasActions::routing, XContentParser::text, new ParseField("routing"), ValueType.INT);
            ADD_PARSER.declareField(AliasActions::indexRouting, XContentParser::text, new ParseField("index_routing"), ValueType.INT);
            ADD_PARSER.declareField(AliasActions::searchRouting, XContentParser::text, new ParseField("search_routing"), ValueType.INT);
        }
        private static final ObjectParser<AliasActions, Void> REMOVE_PARSER = parser("remove", AliasActions::remove);
        private static final ObjectParser<AliasActions, Void> REMOVE_INDEX_PARSER = parser("remove_index", AliasActions::removeIndex);

        /**
         * Parser for any one {@link AliasAction}.
         */
        public static final ConstructingObjectParser<AliasActions, Void> PARSER = new ConstructingObjectParser<>(
                "alias_action", a -> {
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
            PARSER.declareObject(optionalConstructorArg(), ADD_PARSER, new ParseField("add"));
            PARSER.declareObject(optionalConstructorArg(), REMOVE_PARSER, new ParseField("remove"));
            PARSER.declareObject(optionalConstructorArg(), REMOVE_INDEX_PARSER, new ParseField("remove_index"));
        }

        private final AliasActions.Type type;
        private String[] indices;
        private String[] aliases = Strings.EMPTY_ARRAY;
        private String filter;
        private String routing;
        private String indexRouting;
        private String searchRouting;

        AliasActions(AliasActions.Type type) {
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
            this.indices = new String[] {index};
            return this;
        }

        /**
         * Aliases to use with this action.
         */
        @Override
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
            this.aliases = new String[] {alias};
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
                this.filter = builder.string();
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
                this.filter = builder.string();
                return this;
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to build json for alias request", e);
            }
        }

        @Override
        public String[] aliases() {
            return aliases;
        }

        @Override
        public boolean expandAliasesWildcards() {
            //remove operations support wildcards among aliases, add operations don't
            return type == Type.REMOVE;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public String toString() {
            return "AliasActions["
                    + "type=" + type
                    + ",indices=" + Arrays.toString(indices)
                    + ",aliases=" + Arrays.deepToString(aliases)
                    + ",filter=" + filter
                    + ",routing=" + routing
                    + ",indexRouting=" + indexRouting
                    + ",searchRouting=" + searchRouting
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
                    && Objects.equals(searchRouting, other.searchRouting);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, indices, aliases, filter, routing, indexRouting, searchRouting);
        }
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        allAliasActions = in.readList(AliasActions::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(allAliasActions);
    }

    public IndicesOptions indicesOptions() {
        return INDICES_OPTIONS;
    }
}
