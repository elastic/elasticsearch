/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.rest.action.termvector;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;

/**
 * This class parses the json request and translates it into a
 * TermVectorRequest.
 */
public class RestTermVectorAction extends BaseRestHandler {

    @Inject
    public RestTermVectorAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_termvector", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_termvector", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        TermVectorRequest termVectorRequest = new TermVectorRequest(request.param("index"), request.param("type"), request.param("id"));
        termVectorRequest.routing(request.param("routing"));
        termVectorRequest.parent(request.param("parent"));
        termVectorRequest.preference(request.param("preference"));
        if (request.hasContent()) {
            try {
                parseRequest(request.content(), termVectorRequest);
            } catch (IOException e1) {
                Set<String> selectedFields = termVectorRequest.selectedFields();
                String fieldString = "all";
                if (selectedFields != null) {
                    Strings.arrayToDelimitedString(termVectorRequest.selectedFields().toArray(new String[1]), " ");
                }
                logger.error("Something is wrong with your parameters for the term vector request. I am using parameters "
                        + "\n positions :" + termVectorRequest.positions() + "\n offsets :" + termVectorRequest.offsets() + "\n payloads :"
                        + termVectorRequest.payloads() + "\n termStatistics :" + termVectorRequest.termStatistics()
                        + "\n fieldStatistics :" + termVectorRequest.fieldStatistics() + "\nfields " + fieldString, (Object) null);
            }
        }
        readURIParameters(termVectorRequest, request);

        client.termVector(termVectorRequest, new ActionListener<TermVectorResponse>() {
            @Override
            public void onResponse(TermVectorResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    response.toXContent(builder, request);
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }

    static public void readURIParameters(TermVectorRequest termVectorRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorRequest, fields);
        termVectorRequest.offsets(request.paramAsBoolean("offsets", termVectorRequest.offsets()));
        termVectorRequest.positions(request.paramAsBoolean("positions", termVectorRequest.positions()));
        termVectorRequest.payloads(request.paramAsBoolean("payloads", termVectorRequest.payloads()));
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
                    selectedFields = new HashSet<String>();
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

    static public void parseRequest(BytesReference cont, TermVectorRequest termVectorRequest) throws IOException {

        XContentParser parser = XContentFactory.xContent(cont).createParser(cont);
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            List<String> fields = new ArrayList<String>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName != null) {
                    if (currentFieldName.equals("fields")) {

                        if (token == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                fields.add(parser.text());
                            }
                        } else {
                            throw new ElasticSearchParseException(
                                    "The parameter fields must be given as an array! Use syntax : \"fields\" : [\"field1\", \"field2\",...]");
                        }
                    } else if (currentFieldName.equals("offsets")) {
                        termVectorRequest.offsets(parser.booleanValue());
                    } else if (currentFieldName.equals("positions")) {
                        termVectorRequest.positions(parser.booleanValue());
                    } else if (currentFieldName.equals("payloads")) {
                        termVectorRequest.payloads(parser.booleanValue());
                    } else if (currentFieldName.equals("term_statistics") || currentFieldName.equals("termStatistics")) {
                        termVectorRequest.termStatistics(parser.booleanValue());
                    } else if (currentFieldName.equals("field_statistics") || currentFieldName.equals("fieldStatistics")) {
                        termVectorRequest.fieldStatistics(parser.booleanValue());
                    } else {
                        throw new ElasticSearchParseException("The parameter " + currentFieldName
                                + " is not valid for term vector request!");
                    }
                }
            }
            String[] fieldsAsArray = new String[fields.size()];
            termVectorRequest.selectedFields(fields.toArray(fieldsAsArray));
        } finally {
            parser.close();
        }
    }
}
