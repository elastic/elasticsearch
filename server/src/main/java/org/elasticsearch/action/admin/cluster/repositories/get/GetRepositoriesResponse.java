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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Get repositories response
 */
public class GetRepositoriesResponse extends ActionResponse implements ToXContentObject {

    private final RepositoriesMetaData repositories;

    GetRepositoriesResponse(RepositoriesMetaData repositories) {
        this.repositories = repositories;
    }

    public GetRepositoriesResponse(StreamInput in) throws IOException {
        repositories = new RepositoriesMetaData(in);
    }

    /**
     * List of repositories to return
     *
     * @return list or repositories
     */
    public List<RepositoryMetaData> repositories() {
        return repositories.repositories();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositories.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositories.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static GetRepositoriesResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return new GetRepositoriesResponse(RepositoriesMetaData.fromXContent(parser));
    }
}
