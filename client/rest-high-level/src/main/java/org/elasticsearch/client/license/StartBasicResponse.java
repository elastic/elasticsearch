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
package org.elasticsearch.client.license;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class StartBasicResponse {

    private static final ConstructingObjectParser<StartBasicResponse, Void> PARSER = new ConstructingObjectParser<>(
        "start_basic_response", true, (a, v) -> {
        boolean basicWasStarted = (Boolean) a[0];
        String errorMessage = (String) a[1];

        if (basicWasStarted) {
            return new StartBasicResponse(StartBasicResponse.Status.GENERATED_BASIC);
        }
        StartBasicResponse.Status status = StartBasicResponse.Status.fromErrorMessage(errorMessage);
        @SuppressWarnings("unchecked") Tuple<String, Map<String, String[]>> acknowledgements = (Tuple<String, Map<String, String[]>>) a[2];
        return new StartBasicResponse(status, acknowledgements.v2(), acknowledgements.v1());
    });

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("basic_was_started"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("error_message"));
        PARSER.declareObject(optionalConstructorArg(), (parser, v) -> {
                Map<String, String[]> acknowledgeMessages = new HashMap<>();
                String message = null;
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (currentFieldName == null) {
                            throw new XContentParseException(parser.getTokenLocation(), "expected message header or acknowledgement");
                        }
                        if (new ParseField("message").getPreferredName().equals(currentFieldName)) {
                            ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, parser::getTokenLocation);
                            message = parser.text();
                        } else {
                            if (token != XContentParser.Token.START_ARRAY) {
                                throw new XContentParseException(parser.getTokenLocation(), "unexpected acknowledgement type");
                            }
                            List<String> acknowledgeMessagesList = new ArrayList<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, parser::getTokenLocation);
                                acknowledgeMessagesList.add(parser.text());
                            }
                            acknowledgeMessages.put(currentFieldName, acknowledgeMessagesList.toArray(new String[0]));
                        }
                    }
                }
                return new Tuple<>(message, acknowledgeMessages);
            }, new ParseField("acknowledge"));
    }

    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeMessage;

    public enum Status {
        GENERATED_BASIC(true, null),
        ALREADY_USING_BASIC(false, "Operation failed: Current license is basic."),
        NEED_ACKNOWLEDGEMENT(false, "Operation failed: Needs acknowledgement.");

        private final boolean isBasicStarted;
        private final String errorMessage;

        Status(boolean isBasicStarted, String errorMessage) {
            this.isBasicStarted = isBasicStarted;
            this.errorMessage = errorMessage;
        }

        static StartBasicResponse.Status fromErrorMessage(final String errorMessage) {
            final StartBasicResponse.Status[] values = StartBasicResponse.Status.values();
            for (StartBasicResponse.Status status : values) {
                if (Objects.equals(status.errorMessage, errorMessage)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("No status for error message ['" + errorMessage + "']");
        }
    }

    private StartBasicResponse.Status status;

    private StartBasicResponse(StartBasicResponse.Status status) {
        this(status, Collections.emptyMap(), null);
    }

    private StartBasicResponse(StartBasicResponse.Status status,
                              Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        this.status = status;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    public Status getStatus() {
        return status;
    }

    public boolean isAcknowledged() {
        return status != StartBasicResponse.Status.NEED_ACKNOWLEDGEMENT;
    }

    public boolean isBasicStarted() {
        return status.isBasicStarted;
    }

    public String getErrorMessage() {
        return status.errorMessage;
    }

    public String getAcknowledgeMessage() {
        return acknowledgeMessage;
    }

    public Map<String, String[]> getAcknowledgeMessages() {
        return acknowledgeMessages;
    }

    public static StartBasicResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
