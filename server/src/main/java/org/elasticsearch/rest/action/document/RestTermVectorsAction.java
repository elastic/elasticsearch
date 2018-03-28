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

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * This class parses the json request and translates it into a
 * TermVectorsRequest.
 */
public class RestTermVectorsAction extends BaseRestHandler {
    public RestTermVectorsAction(Settings settings, RestController controller) {
        super(settings);
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
    public String getName() {
        return "document_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("type"), request.param("id"));
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                TermVectorsRequest.parseRequest(termVectorsRequest, parser);
            }
        }
        readURIParameters(termVectorsRequest, request);

        return channel -> client.termVectors(termVectorsRequest, new RestToXContentListener<>(channel));
    }

    public static void readURIParameters(TermVectorsRequest termVectorsRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorsRequest, fields);
        termVectorsRequest.offsets(request.paramAsBoolean("offsets", termVectorsRequest.offsets()));
        termVectorsRequest.positions(request.paramAsBoolean("positions", termVectorsRequest.positions()));
        termVectorsRequest.payloads(request.paramAsBoolean("payloads", termVectorsRequest.payloads()));
        termVectorsRequest.routing(request.param("routing"));
        termVectorsRequest.realtime(request.paramAsBoolean("realtime", termVectorsRequest.realtime()));
        termVectorsRequest.version(RestActions.parseVersion(request, termVectorsRequest.version()));
        termVectorsRequest.versionType(VersionType.fromString(request.param("version_type"), termVectorsRequest.versionType()));
        termVectorsRequest.parent(request.param("parent"));
        termVectorsRequest.preference(request.param("preference"));
        termVectorsRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorsRequest.fieldStatistics()));
    }

    public static void addFieldStringsFromParameter(TermVectorsRequest termVectorsRequest, String fields) {
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
