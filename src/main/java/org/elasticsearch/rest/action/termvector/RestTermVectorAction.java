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

package org.elasticsearch.rest.action.termvector;

import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * This class parses the json request and translates it into a
 * TermVectorRequest.
 */
public class RestTermVectorAction extends BaseRestHandler {

    @Inject
    public RestTermVectorAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/_termvector", this);
        controller.registerHandler(POST, "/{index}/{type}/_termvector", this);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_termvector", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_termvector", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        TermVectorRequest termVectorRequest = new TermVectorRequest(request.param("index"), request.param("type"), request.param("id"));
        XContentParser parser = null;
        if (request.hasContent()) {
            try {
                parser = XContentFactory.xContent(request.content()).createParser(request.content());
                TermVectorRequest.parseRequest(termVectorRequest, parser);
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }
        }
        readURIParameters(termVectorRequest, request);

        client.termVector(termVectorRequest, new RestToXContentListener<TermVectorResponse>(channel));
    }

    static public void readURIParameters(TermVectorRequest termVectorRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorRequest, fields);
        termVectorRequest.offsets(request.paramAsBoolean("offsets", termVectorRequest.offsets()));
        termVectorRequest.positions(request.paramAsBoolean("positions", termVectorRequest.positions()));
        termVectorRequest.payloads(request.paramAsBoolean("payloads", termVectorRequest.payloads()));
        termVectorRequest.routing(request.param("routing"));
        termVectorRequest.parent(request.param("parent"));
        termVectorRequest.preference(request.param("preference"));
        termVectorRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorRequest.termStatistics()));
        termVectorRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorRequest.termStatistics()));
        termVectorRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorRequest.fieldStatistics()));
        termVectorRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorRequest.fieldStatistics()));
    }

    static public void addFieldStringsFromParameter(TermVectorRequest termVectorRequest, String fields) {
        Set<String> selectedFields = termVectorRequest.selectedFields();
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
            termVectorRequest.selectedFields(selectedFields.toArray(new String[selectedFields.size()]));
        }
    }

}
