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

package org.elasticsearch.rest.action.deletebyquery;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.action.deletebyquery.DeleteByQueryAction.INSTANCE;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * @see DeleteByQueryRequest
 */
public class RestDeleteByQueryAction extends BaseRestHandler {

    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestDeleteByQueryAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(DELETE, "/{index}/_query", this);
        controller.registerHandler(DELETE, "/{index}/{type}/_query", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws IOException {
        DeleteByQueryRequest delete = new DeleteByQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        delete.indicesOptions(IndicesOptions.fromRequest(request, delete.indicesOptions()));
        delete.routing(request.param("routing"));
        if (request.hasParam("timeout")) {
            delete.timeout(request.paramAsTime("timeout", null));
        }
        if (request.hasContent()) {
            XContentParser requestParser = XContentFactory.xContent(request.content()).createParser(request.content());
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
            context.reset(requestParser);
            context.parseFieldMatcher(parseFieldMatcher);
            final QueryBuilder<?> builder = context.parseInnerQueryBuilder();
            delete.query(builder);
        } else {
            String source = request.param("source");
            if (source != null) {
                XContentParser requestParser = XContentFactory.xContent(source).createParser(source);
                QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
                context.reset(requestParser);
                final QueryBuilder<?> builder = context.parseInnerQueryBuilder();
                delete.query(builder);
            } else {
                QueryBuilder<?> queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                if (queryBuilder != null) {
                    delete.query(queryBuilder);
                }
            }
        }
        delete.types(Strings.splitStringByCommaToArray(request.param("type")));
        client.execute(INSTANCE, delete, new RestToXContentListener<DeleteByQueryResponse>(channel));
    }
}
