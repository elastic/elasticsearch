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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasAction.Type;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.AliasAction.readAliasAction;

/**
 * A request to add/remove aliases for one or more indices.
 */
public class IndicesAliasesRequest extends AcknowledgedRequest<IndicesAliasesRequest> implements CompositeIndicesRequest {

    private List<AliasActions> allAliasActions = Lists.newArrayList();

    //indices options that require every specified index to exist, expand wildcards only to open indices and
    //don't allow that no indices are resolved from wildcard expressions
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, false);

    public IndicesAliasesRequest() {

    }

    /*
     * Aliases can be added by passing multiple indices to the Request and
     * deleted by passing multiple indices and aliases. They are expanded into
     * distinct AliasAction instances when the request is processed. This class
     * holds the AliasAction and in addition the arrays or alias names and
     * indices that is later used to create the final AliasAction instances.
     */
    public static class AliasActions implements AliasesRequest {
        private String[] indices = Strings.EMPTY_ARRAY;
        private String[] aliases = Strings.EMPTY_ARRAY;
        private AliasAction aliasAction;

        public AliasActions(AliasAction.Type type, String[] indices, String[] aliases) {
            aliasAction = new AliasAction(type);
            indices(indices);
            aliases(aliases);
        }
        
        public AliasActions(AliasAction.Type type, String index, String alias) {
            aliasAction = new AliasAction(type);
            indices(index);
            aliases(alias);
        }
        
        AliasActions(AliasAction.Type type, String[] index, String alias) {
            aliasAction = new AliasAction(type);
            indices(index);
            aliases(alias);
        }
        
        public AliasActions(AliasAction action) {
            this.aliasAction = action;
            indices(action.index());
            aliases(action.alias());
        }

        public AliasActions(Type type, String index, String[] aliases) {
            aliasAction = new AliasAction(type);
            indices(index);
            aliases(aliases);
        }

        public AliasActions() {
        }

        public AliasActions filter(Map<String, Object> filter) {
            aliasAction.filter(filter);
            return this;
        }
        
        public AliasActions filter(FilterBuilder filter) {
            aliasAction.filter(filter);
            return this;
        }

        public Type actionType() {
            return aliasAction.actionType();
        }

        public void routing(String routing) {
            aliasAction.routing(routing);
        }

        public void searchRouting(String searchRouting) {
            aliasAction.searchRouting(searchRouting);
        }

        public void indexRouting(String indexRouting) {
            aliasAction.indexRouting(indexRouting);
        }

        public AliasActions filter(String filter) {
            aliasAction.filter(filter);
            return this;
        }

        @Override
        public AliasActions indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public AliasActions aliases(String... aliases) {
            this.aliases = aliases;
            return this;
        }

        @Override
        public String[] aliases() {
            return aliases;
        }

        @Override
        public boolean expandAliasesWildcards() {
            //remove operations support wildcards among aliases, add operations don't
            return aliasAction.actionType() == Type.REMOVE;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        public AliasAction aliasAction() {
            return aliasAction;
        }

        public String[] concreteAliases(MetaData metaData, String concreteIndex) {
            if (expandAliasesWildcards()) {
                //for DELETE we expand the aliases
                String[] indexAsArray = {concreteIndex};
                ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliasMetaData = metaData.findAliases(aliases, indexAsArray);
                List<String> finalAliases = new ArrayList<>();
                for (ObjectCursor<ImmutableList<AliasMetaData>> curAliases : aliasMetaData.values()) {
                    for (AliasMetaData aliasMeta: curAliases.value) {
                        finalAliases.add(aliasMeta.alias());
                    }
                }
                return finalAliases.toArray(new String[finalAliases.size()]);
            } else {
                //for add we just return the current aliases
                return aliases;
            }
        }
        public AliasActions readFrom(StreamInput in) throws IOException {
            indices = in.readStringArray();
            aliases = in.readStringArray();
            aliasAction = readAliasAction(in);
            return this;
        }
        
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(indices);
            out.writeStringArray(aliases);
            this.aliasAction.writeTo(out);
        }
    }

    /**
     * Adds an alias to the index.
     * @param alias The alias
     * @param indices The indices
     */
    public IndicesAliasesRequest addAlias(String alias, String... indices) {
        addAliasAction(new AliasActions(AliasAction.Type.ADD, indices, alias));
        return this;
    }


    public void addAliasAction(AliasActions aliasAction) {
        allAliasActions.add(aliasAction);
    }


    public IndicesAliasesRequest addAliasAction(AliasAction action) {
        addAliasAction(new AliasActions(action));
        return this;
    }
    
    /**
     * Adds an alias to the index.
     * @param alias  The alias
     * @param filter The filter
     * @param indices  The indices
     */
    public IndicesAliasesRequest addAlias(String alias, Map<String, Object> filter, String... indices) {
        addAliasAction(new AliasActions(AliasAction.Type.ADD, indices, alias).filter(filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     * @param alias         The alias
     * @param filterBuilder The filter
     * @param indices         The indices
     */
    public IndicesAliasesRequest addAlias(String alias, FilterBuilder filterBuilder, String... indices) {
        addAliasAction(new AliasActions(AliasAction.Type.ADD, indices, alias).filter(filterBuilder));
        return this;
    }
    
    
    /**
     * Removes an alias to the index.
     *
     * @param indices The indices
     * @param aliases The aliases
     */
    public IndicesAliasesRequest removeAlias(String[] indices, String... aliases) {
        addAliasAction(new AliasActions(AliasAction.Type.REMOVE, indices, aliases));
        return this;
    }
    
    /**
     * Removes an alias to the index.
     *
     * @param index The index
     * @param aliases The aliases
     */
    public IndicesAliasesRequest removeAlias(String index, String... aliases) {
        addAliasAction(new AliasActions(AliasAction.Type.REMOVE, index, aliases));
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
        for (AliasActions aliasAction : allAliasActions) {
            if (aliasAction.actionType() == AliasAction.Type.ADD) {
                if (aliasAction.aliases.length != 1) {
                    validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                            + "] requires exactly one [alias] to be set", validationException);
                }
                if (!Strings.hasText(aliasAction.aliases[0])) {
                    validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                            + "] requires an [alias] to be set", validationException);
                }
            } else {
                if (aliasAction.aliases.length == 0) {
                    validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                            + "]: aliases may not be empty", validationException);
                }
                for (String alias : aliasAction.aliases) {
                    if (!Strings.hasText(alias)) {
                        validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                                + "]: [alias] may not be empty string", validationException);
                    }
                }
            }
            if (CollectionUtils.isEmpty(aliasAction.indices)) {
                validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                        + "]: Property [index] was either missing or null", validationException);
            } else {
                for (String index : aliasAction.indices) {
                    if (!Strings.hasText(index)) {
                        validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH)
                                + "]: [index] may not be empty string", validationException);
                    }
                }
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            allAliasActions.add(readAliasActions(in));
        }
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(allAliasActions.size());
        for (AliasActions aliasAction : allAliasActions) {
            aliasAction.writeTo(out);
        }
        writeTimeout(out);
    }

    public IndicesOptions indicesOptions() {
        return INDICES_OPTIONS;
    }
    
    private static AliasActions readAliasActions(StreamInput in) throws IOException {
        AliasActions actions = new AliasActions();
        return actions.readFrom(in);
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        return allAliasActions;
    }
}
