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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.common.ProtocolUtils;
import org.elasticsearch.rest.RestStatus;

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

public class PostStartBasicResponse extends AcknowledgedResponse {

    private static final ConstructingObjectParser<PostStartBasicResponse, Void> PARSER = new ConstructingObjectParser<>(
        "post_start_basic_response", true, (a, v) -> {
        boolean basicWasStarted = (Boolean) a[0];
        String errorMessage = (String) a[1];

        if (basicWasStarted) {
            return new PostStartBasicResponse(Status.GENERATED_BASIC);
        }
        Status status = Status.fromErrorMessage(errorMessage);
        @SuppressWarnings("unchecked") Tuple<String, Map<String, String[]>> acknowledgements = (Tuple<String, Map<String, String[]>>) a[2];
        return new PostStartBasicResponse(status, acknowledgements.v2(), acknowledgements.v1());
    });

    private static final ParseField BASIC_WAS_STARTED = new ParseField("basic_was_started");
    private static final ParseField ERROR_MESSAGE = new ParseField("error_message");
    private static final ParseField ACKNOWLEDGE = new ParseField("acknowledge");
    private static final ParseField MESSAGE = new ParseField("message");

    static {
        PARSER.declareBoolean(constructorArg(), BASIC_WAS_STARTED);
        PARSER.declareString(optionalConstructorArg(), ERROR_MESSAGE);
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
                        if (MESSAGE.getPreferredName().equals(currentFieldName)) {
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
            },
            ACKNOWLEDGE);
    }

    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeMessage;

    public enum Status {
        GENERATED_BASIC(true, null, RestStatus.OK),
        ALREADY_USING_BASIC(false, "Operation failed: Current license is basic.", RestStatus.FORBIDDEN),
        NEED_ACKNOWLEDGEMENT(false, "Operation failed: Needs acknowledgement.", RestStatus.OK);

        private final boolean isBasicStarted;
        private final String errorMessage;
        private final RestStatus restStatus;

        Status(boolean isBasicStarted, String errorMessage, RestStatus restStatus) {
            this.isBasicStarted = isBasicStarted;
            this.errorMessage = errorMessage;
            this.restStatus = restStatus;
        }

        public boolean isBasicStarted() {
            return isBasicStarted;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }

        static Status fromErrorMessage(final String errorMessage) {
            final Status[] values = Status.values();
            for (Status status : values) {
                if (Objects.equals(status.errorMessage, errorMessage)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("No status for error message ['" + errorMessage + "']");
        }
    }

    private Status status;

    public PostStartBasicResponse() {
    }

    public PostStartBasicResponse(Status status) {
        this(status, Collections.emptyMap(), null);
    }

    public PostStartBasicResponse(Status status, Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        super(status != Status.NEED_ACKNOWLEDGEMENT);
        this.status = status;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    public Status status() {
        return status;
    }

    public String acknowledgeMessage() {
        return acknowledgeMessage;
    }

    public Map<String, String[]> acknowledgeMessages() {
        return acknowledgeMessages;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readEnum(Status.class);
        acknowledgeMessage = in.readOptionalString();
        int size = in.readVInt();
        Map<String, String[]> acknowledgeMessages = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String feature = in.readString();
            int nMessages = in.readVInt();
            String[] messages = new String[nMessages];
            for (int j = 0; j < nMessages; j++) {
                messages[j] = in.readString();
            }
            acknowledgeMessages.put(feature, messages);
        }
        this.acknowledgeMessages = acknowledgeMessages;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(status);
        out.writeOptionalString(acknowledgeMessage);
        out.writeVInt(acknowledgeMessages.size());
        for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().length);
            for (String message : entry.getValue()) {
                out.writeString(message);
            }
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (status.isBasicStarted()) {
            builder.field(BASIC_WAS_STARTED.getPreferredName(), true);
        } else {
            builder.field(BASIC_WAS_STARTED.getPreferredName(), false);
            builder.field(ERROR_MESSAGE.getPreferredName(), status.getErrorMessage());
        }
        if (acknowledgeMessages.isEmpty() == false) {
            builder.startObject(ACKNOWLEDGE.getPreferredName());
            builder.field(MESSAGE.getPreferredName(), acknowledgeMessage);
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

    public static PostStartBasicResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof PostStartBasicResponse) == false || super.equals(o) == false) return false;
        PostStartBasicResponse that = (PostStartBasicResponse) o;
        return status == that.status
            && Objects.equals(acknowledgeMessage, that.acknowledgeMessage)
            && ProtocolUtils.equals(acknowledgeMessages, that.acknowledgeMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ProtocolUtils.hashCode(acknowledgeMessages), acknowledgeMessage, status);
    }
}
