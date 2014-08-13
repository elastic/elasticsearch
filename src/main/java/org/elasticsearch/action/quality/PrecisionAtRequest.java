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

package org.elasticsearch.action.quality;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PrecisionAtRequest extends ActionRequest<PrecisionAtRequest> {

    /** TODO move the following to a metric specific context - need move writing and reading too then. */

    /** IDs of documents considered relevant for this query. */
    private List<String> relevantDocs = new LinkedList<String>();
    /** QueryBuilder to run against search index and generate hits to compare against relevantDocs. */
    private BytesReference query;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void queryBuilder(BytesReference query) {
        this.query = query;
    }

    public void relevantDocs(Set<String> relevant) {
        this.relevantDocs.addAll(relevant);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        query = in.readBytesReference();
        relevantDocs = (List<String>) in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(query);
        out.writeGenericValue(relevantDocs);
    }
    
    @Override
    public String toString() {
        ToStringHelper help = Objects.toStringHelper(this).add("Relevant docs", relevantDocs.toString());
        help.add("Query", query);
        return help.toString();
    }
}
