/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.cluster.metadata.AliasAction.*;

/**
 * A request to add/remove aliases for one or more indices.
 *
 * @author kimchy (shay.banon)
 */
public class IndicesAliasesRequest extends MasterNodeOperationRequest {

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
     * Removes an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequest removeAlias(String index, String alias) {
        aliasActions.add(new AliasAction(AliasAction.Type.REMOVE, index, alias));
        return this;
    }

    List<AliasAction> aliasActions() {
        return this.aliasActions;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (aliasActions.isEmpty()) {
            validationException = addValidationError("Must specify at least one alias action", validationException);
        }
        return validationException;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            aliasActions.add(readAliasAction(in));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(aliasActions.size());
        for (AliasAction aliasAction : aliasActions) {
            aliasAction.writeTo(out);
        }
    }
}
