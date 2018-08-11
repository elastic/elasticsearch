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

package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.common.ProtocolUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PostStartTrialResponse extends AcknowledgedResponse {

    private static final ParseField TRIAL_WAS_STARTED_FIELD = new ParseField("trial_was_started");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField ERROR_MESSAGE_FIELD = new ParseField("error_message");
    private static final ParseField ACKNOWLEDGE_DETAILS_FIELD = new ParseField("acknowledge");
    private static final ParseField ACKNOWLEDGE_HEADER_FIELD = new ParseField("message");

    private static final ConstructingObjectParser<PostStartTrialResponse, Void> PARSER = new ConstructingObjectParser<>(
        "post_start_trial_response",
        true,
        (arguments, aVoid) -> {
            boolean acknowledged = (boolean) arguments[0];
            boolean trialWasStarted = (boolean) arguments[1];
            String type = (String) arguments[2];
            String errorMessage = (String) arguments[3];
            @SuppressWarnings("unchecked")
            Tuple<String, Map<String, String[]>> acknowledgeDetails = (Tuple<String, Map<String, String[]>>) arguments[4];

            if (trialWasStarted) {
                return new PostStartTrialResponse(acknowledged, type);
            } else {
                return new PostStartTrialResponse(acknowledged, errorMessage, acknowledgeDetails.v1(), acknowledgeDetails.v2());
            }
        }
    );

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareBoolean(constructorArg(), TRIAL_WAS_STARTED_FIELD);
        PARSER.declareString(optionalConstructorArg(), TYPE_FIELD);
        PARSER.declareString(optionalConstructorArg(), ERROR_MESSAGE_FIELD);
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
                    if ("message".equals(currentFieldName)) {
                        if (token != XContentParser.Token.VALUE_STRING) {
                            throw new XContentParseException(parser.getTokenLocation(), "unexpected message header type");
                        }
                        message = parser.text();
                    } else {
                        if (token != XContentParser.Token.START_ARRAY) {
                            throw new XContentParseException(parser.getTokenLocation(), "unexpected acknowledgement type");
                        }
                        List<String> acknowledgeMessagesList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token != XContentParser.Token.VALUE_STRING) {
                                throw new XContentParseException(parser.getTokenLocation(), "unexpected acknowledgement text");
                            }
                            acknowledgeMessagesList.add(parser.text());
                        }
                        acknowledgeMessages.put(currentFieldName, acknowledgeMessagesList.toArray(new String[0]));
                    }
                }
            }
            return new Tuple<>(message, acknowledgeMessages);
        }, ACKNOWLEDGE_DETAILS_FIELD);
    }

    private boolean trialWasStarted;
    private String type;
    private String errorMessage;
    private String acknowledgeMessage;
    private Map<String, String[]> acknowledgeMessages;

    public PostStartTrialResponse() {}

    public PostStartTrialResponse(boolean acknowledged, String type) {
        this(acknowledged, true, type, null, null, Collections.emptyMap());

        Objects.requireNonNull(type);
    }

    public PostStartTrialResponse(boolean acknowledged,
                                  String errorMessage,
                                  String acknowledgeMessage,
                                  Map<String, String[]> acknowledgeMessages) {
        this(acknowledged, false, null, errorMessage, acknowledgeMessage, acknowledgeMessages);

        Objects.requireNonNull(errorMessage);
        Objects.requireNonNull(acknowledgeMessage);
        Objects.requireNonNull(acknowledgeMessages);
    }

    private PostStartTrialResponse(boolean acknowledged,
                                  boolean trialWasStarted,
                                  String type,
                                  String errorMessage,
                                  String acknowledgeMessage,
                                  Map<String, String[]> acknowledgeMessages) {

        super(acknowledged);
        this.trialWasStarted = trialWasStarted;
        this.type = type;
        this.errorMessage = errorMessage;
        this.acknowledgeMessage = acknowledgeMessage;
        this.acknowledgeMessages = acknowledgeMessages;
    }

    public boolean isTrialWasStarted() {
        return trialWasStarted;
    }

    public String getType() {
        return type;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getAcknowledgeMessage() {
        return acknowledgeMessage;
    }

    public Map<String, String[]> getAcknowledgeMessages() {
        return acknowledgeMessages;
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(TRIAL_WAS_STARTED_FIELD.getPreferredName(), trialWasStarted);
        if (trialWasStarted) {
            builder.field(TYPE_FIELD.getPreferredName(), type);
        } else {
            builder.field(ERROR_MESSAGE_FIELD.getPreferredName(), errorMessage);
        }

        if (acknowledgeMessages.isEmpty() == false) {
            builder.startObject(ACKNOWLEDGE_DETAILS_FIELD.getPreferredName());
            builder.field(ACKNOWLEDGE_HEADER_FIELD.getPreferredName(), acknowledgeMessage);
            for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
                builder.startArray(entry.getKey());
                for (String message : entry.getValue()) {
                    builder.value(message);
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }

    public static PostStartTrialResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (getClass() != other.getClass()) {
            return false;
        }

        if (super.equals(other) == false) {
            return false;
        }

        PostStartTrialResponse otherResponse = (PostStartTrialResponse) other;

        return Objects.equals(acknowledged, otherResponse.acknowledged)
            && Objects.equals(trialWasStarted, otherResponse.trialWasStarted)
            && Objects.equals(type, otherResponse.type)
            && Objects.equals(acknowledgeMessage, otherResponse.acknowledgeMessage)
            && ProtocolUtils.equals(acknowledgeMessages, otherResponse.acknowledgeMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            acknowledged,
            trialWasStarted,
            type,
            acknowledgeMessage,
            ProtocolUtils.hashCode(acknowledgeMessages)
        );
    }

}
