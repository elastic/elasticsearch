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
package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request that deletes a index warmer (name, {@link org.elasticsearch.action.search.SearchRequest})
 * tuple from the clusters metadata.
 */
public class DeleteWarmerRequest extends AcknowledgedRequest<DeleteWarmerRequest> implements IndicesRequest.Replaceable {

    private String[] names = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);
    private String[] indices = Strings.EMPTY_ARRAY;

    public DeleteWarmerRequest() {
    }

    /**
     * Constructs a new delete warmer request for the specified name.
     *
     * @param names: the name (or wildcard expression) of the warmer to match, null to delete all.
     */
    public DeleteWarmerRequest(String... names) {
        names(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(names)) {
            validationException = addValidationError("warmer names are missing", validationException);
        } else {
            validationException = checkForEmptyString(validationException, names);
        }
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("indices are missing", validationException);
        } else {
            validationException = checkForEmptyString(validationException, indices);
        }
        return validationException;
    }

    private ActionRequestValidationException checkForEmptyString(ActionRequestValidationException validationException, String[] strings) {
        boolean containsEmptyString = false;
        for (String string : strings) {
            if (!Strings.hasText(string)) {
                containsEmptyString = true;
            }
        }
        if (containsEmptyString) {
            validationException = addValidationError("types must not contain empty strings", validationException);
        }
        return validationException;
    }

    /**
     * The name to delete.
     */
    @Nullable
    public String[] names() {
        return names;
    }

    /**
     * The name (or wildcard expression) of the index warmer to delete, or null
     * to delete all warmers.
     */
    public DeleteWarmerRequest names(@Nullable String... names) {
        this.names = names;
        return this;
    }

    /**
     * Sets the indices this put mapping operation will execute on.
     */
    @Override
    public DeleteWarmerRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The indices the mappings will be put.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteWarmerRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        names = in.readStringArray();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(names);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        writeTimeout(out);
    }
}
