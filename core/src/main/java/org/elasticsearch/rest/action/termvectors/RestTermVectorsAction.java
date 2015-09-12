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

package org.elasticsearch.rest.action.termvectors;

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * This class parses the json request and translates it into a
 * TermVectorsRequest.
 */
public class RestTermVectorsAction extends BaseRestHandler {

    @Inject
    public RestTermVectorsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/_termvectors", this);
        controller.registerHandler(POST, "/{index}/{type}/_termvectors", this);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_termvectors", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_termvectors", this);

        // we keep usage of _termvector as alias for now
        controller.registerHandler(GET, "/{index}/{type}/_termvector", this);
        controller.registerHandler(POST, "/{index}/{type}/_termvector", this);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_termvector", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_termvector", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("type"), request.param("id"));
        if (RestActions.hasBodyContent(request)) {
            try (XContentParser parser = XContentFactory.xContent(RestActions.guessBodyContentType(request)).createParser(RestActions.getRestContent(request))){
                TermVectorsRequest.parseRequest(termVectorsRequest, parser);
            }
        }
        readURIParameters(termVectorsRequest, request);

        client.termVectors(termVectorsRequest, new RestToXContentListener<TermVectorsResponse>(channel));
    }

    static public void readURIParameters(TermVectorsRequest termVectorsRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorsRequest, fields);
        termVectorsRequest.offsets(request.paramAsBoolean("offsets", termVectorsRequest.offsets()));
        termVectorsRequest.positions(request.paramAsBoolean("positions", termVectorsRequest.positions()));
        termVectorsRequest.payloads(request.paramAsBoolean("payloads", termVectorsRequest.payloads()));
        termVectorsRequest.routing(request.param("routing"));
        termVectorsRequest.realtime(request.paramAsBoolean("realtime", null));
        termVectorsRequest.version(RestActions.parseVersion(request, termVectorsRequest.version()));
        termVectorsRequest.versionType(VersionType.fromString(request.param("version_type"), termVectorsRequest.versionType()));
        termVectorsRequest.parent(request.param("parent"));
        termVectorsRequest.preference(request.param("preference"));
        termVectorsRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.dfs(request.paramAsBoolean("dfs", termVectorsRequest.dfs()));
    }

    static public void addFieldStringsFromParameter(TermVectorsRequest termVectorsRequest, String fields) {
        Set<String> selectedFields = termVectorsRequest.selectedFields();
        if (fields != null) {
            String[] paramFieldStrings = Strings.commaDelimitedListToStringArray(fields);
            for (String field : paramFieldStrings) {
                if (selectedFields == null) {
                    selectedFields = new HashSet<>();
                }
                if (!selectedFields.contains(field)) {
                    field = field.replaceAll("\\s", "");
                    selectedFields.add(field);
                }
            }
        }
        if (selectedFields != null) {
            termVectorsRequest.selectedFields(selectedFields.toArray(new String[selectedFields.size()]));
        }
    }

}
