/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.AliasAction.readAliasAction;

/**
 * A request to add/remove aliases for one or more indices.
 */
public class IndicesAliasesRequest extends AcknowledgedRequest<IndicesAliasesRequest> {

    private List<AliasAction> aliasActions = Lists.newArrayList();

    public IndicesAliasesRequest() {

    }

    /**
     * Adds an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequest addAlias(String index, String alias) {
        aliasActions.add(new AliasAction(AliasAction.Type.ADD, index, alias));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequest addAlias(String index, String alias, String filter) {
        aliasActions.add(new AliasAction(AliasAction.Type.ADD, index, alias, filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequest addAlias(String index, String alias, Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            aliasActions.add(new AliasAction(AliasAction.Type.ADD, index, alias));
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(filter);
            aliasActions.add(new AliasAction(AliasAction.Type.ADD, index, alias, builder.string()));
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * Adds an alias to the index.
     *
     * @param index         The index
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequest addAlias(String index, String alias, FilterBuilder filterBuilder) {
        if (filterBuilder == null) {
            aliasActions.add(new AliasAction(AliasAction.Type.ADD, index, alias));
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.close();
            return addAlias(index, alias, builder.string());
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to build json for alias request", e);
        }
    }

    /**
     * Removes an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequest removeAlias(String index, String alias) {
        aliasActions.add(new AliasAction(AliasAction.Type.REMOVE, index, alias));
        return this;
    }

    public IndicesAliasesRequest addAliasAction(AliasAction action) {
        aliasActions.add(action);
        return this;
    }

    List<AliasAction> aliasActions() {
        return this.aliasActions;
    }

    public List<AliasAction> getAliasActions() {
        return aliasActions();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (aliasActions.isEmpty()) {
            return addValidationError("Must specify at least one alias action", validationException);
        }
        for (AliasAction aliasAction : aliasActions) {
            if (!Strings.hasText(aliasAction.alias())) {
                validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH) + "] requires an [alias] to be set", validationException);
            }
            if (!Strings.hasText(aliasAction.index())) {
                validationException = addValidationError("Alias action [" + aliasAction.actionType().name().toLowerCase(Locale.ENGLISH) + "] requires an [index] to be set", validationException);
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            aliasActions.add(readAliasAction(in));
        }
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(aliasActions.size());
        for (AliasAction aliasAction : aliasActions) {
            aliasAction.writeTo(out);
        }
        writeTimeout(out);
    }
}
