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
package org.elasticsearch.action.template.get;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class GetSearchTemplatesResponse extends ActionResponse {

    private String searchTemplate;

    GetSearchTemplatesResponse() {
    }

    GetSearchTemplatesResponse(String searchTemplate) {
        this.searchTemplate = searchTemplate;
    }

    public String getSearchTemplate() {
        return searchTemplate;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        searchTemplate = in.readString();

/*
        int size = in.readVInt();
        searchTemplates = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0 ; i < size ; i++) {
            searchTemplates.add(0, in.readString());
        }
*/
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(searchTemplate);
        /*out.writeVInt(searchTemplates.size());
        for (String searchTemplate : searchTemplates) {
            out.writeString(searchTemplate);
        }
        */
    }
}
