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

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get repository request
 */
public class GetRepositoriesRequest extends MasterNodeReadRequest<GetRepositoriesRequest> {

    private String[] repositories = Strings.EMPTY_ARRAY;

    public GetRepositoriesRequest() {
    }

    /**
     * Constructs a new get repositories request with a list of repositories.
     * <p>
     * If the list of repositories is empty or it contains a single element "_all", all registered repositories
     * are returned.
     *
     * @param repositories list of repositories
     */
    public GetRepositoriesRequest(String[] repositories) {
        this.repositories = repositories;
    }

    public GetRepositoriesRequest(StreamInput in) throws IOException {
        super(in);
        repositories = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(repositories);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repositories == null) {
            validationException = addValidationError("repositories is null", validationException);
        }
        return validationException;
    }

    /**
     * The names of the repositories.
     *
     * @return list of repositories
     */
    public String[] repositories() {
        return this.repositories;
    }

    /**
     * Sets the list or repositories.
     * <p>
     * If the list of repositories is empty or it contains a single element "_all", all registered repositories
     * are returned.
     *
     * @param repositories list of repositories
     * @return this request
     */
    public GetRepositoriesRequest repositories(String[] repositories) {
        this.repositories = repositories;
        return this;
    }
}
