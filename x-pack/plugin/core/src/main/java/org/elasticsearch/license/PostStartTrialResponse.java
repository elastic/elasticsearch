/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.license.PostStartTrialResponse.Status.NEED_ACKNOWLEDGEMENT;

class PostStartTrialResponse extends AcknowledgedResponse implements StatusToXContentObject {

    // Nodes Prior to 6.3 did not have NEED_ACKNOWLEDGEMENT as part of status
    enum Pre63Status {
        UPGRADED_TO_TRIAL,
        TRIAL_ALREADY_ACTIVATED;
    }
    enum Status {
        UPGRADED_TO_TRIAL(true, null, RestStatus.OK),
        TRIAL_ALREADY_ACTIVATED(false, "Operation failed: Trial was already activated.", RestStatus.FORBIDDEN),
        NEED_ACKNOWLEDGEMENT(false,"Operation failed: Needs acknowledgement.", RestStatus.OK);

        private final boolean isTrialStarted;

        private final String errorMessage;
        private final RestStatus restStatus;
        Status(boolean isTrialStarted, String errorMessage, RestStatus restStatus) {
            this.isTrialStarted = isTrialStarted;
            this.errorMessage = errorMessage;
            this.restStatus = restStatus;
        }

        boolean isTrialStarted() {
            return isTrialStarted;
        }

        String getErrorMessage() {
            return errorMessage;
        }

        RestStatus getRestStatus() {
            return restStatus;
        }

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        public static Status fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

    }

    private static final ParseField TRIAL_WAS_STARTED_FIELD = new ParseField("trial_was_started");
    private static final ParseField TRIAL_STATUS_FIELD = new ParseField("trial_status");
    private static final ParseField LICENSE_TYPE_FIELD = new ParseField("type");
    private static final ParseField ERROR_MESSAGE_FIELD = new ParseField("error_message");
    private static final ParseField ACKNOWLEDGE_DETAILS_FIELD = new ParseField("acknowledge");
    private static final ParseField ACKNOWLEDGE_HEADER_FIELD = new ParseField("message");

    private static final ConstructingObjectParser<PostStartTrialResponse, Void> PARSER = new ConstructingObjectParser<>(
      "post_start_trial_response", true, (Object[] arguments, Void aVoid) -> {

          // the acknowledge, trial_was_started, and error_message fields are inferred from the trial status
          final Status trialStatus = Status.fromString((String) arguments[0]);
          final String licenseType = (String) arguments[1];

          @SuppressWarnings("unchecked")
          final Tuple<String, Map<String, String[]>> acknowledgeDetails = (Tuple<String, Map<String, String[]>>) arguments[2];

          if (acknowledgeDetails == null) {
              return new PostStartTrialResponse(trialStatus, licenseType);
          } else {
              return new PostStartTrialResponse(trialStatus, licenseType, acknowledgeDetails.v2(), acknowledgeDetails.v1());
          }
    });

    static {
        PARSER.declareString(constructorArg(), TRIAL_STATUS_FIELD);
        PARSER.declareString(constructorArg(), LICENSE_TYPE_FIELD);
        // todo when PutLicenseResponse returns to this project, consolidate this acknowledgement parsing code with it
        PARSER.declareObject(
            optionalConstructorArg(),
            (parser, v) -> {
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
                        if (ACKNOWLEDGE_HEADER_FIELD.getPreferredName().equals(currentFieldName)) {
                            if (token != XContentParser.Token.VALUE_STRING) {
                                throw new XContentParseException(parser.getTokenLocation(), "unexpected message header type");
                            }
                            message = parser.text();
                        } else if (token == XContentParser.Token.START_ARRAY){
                            List<String> acknowledgeMessagesList = new ArrayList<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token != XContentParser.Token.VALUE_STRING) {
                                    throw new XContentParseException(parser.getTokenLocation(), "unexpected acknowledgement text");
                                }
                                acknowledgeMessagesList.add(parser.text());
                            }
                            acknowledgeMessages.put(currentFieldName, acknowledgeMessagesList.toArray(new String[0]));
                        } else {
                            throw new XContentParseException(parser.getTokenLocation(), "unexpected acknowledgement type");
                        }
                    }
                }
                return new Tuple<>(message, acknowledgeMessages);
            },
            ACKNOWLEDGE_DETAILS_FIELD);
    }

    private Status status;
    private String licenseType;
    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeMessage;

    PostStartTrialResponse() {}

    PostStartTrialResponse(Status status, String licenseType) {
        this(status, licenseType, Collections.emptyMap(), null);
    }

    PostStartTrialResponse(Status status, String licenseType, Map<String, String[]> acknowledgeMessages, String acknowledgeMessage) {
        super(status != NEED_ACKNOWLEDGEMENT);

        this.status = status;
        this.licenseType = licenseType;
        this.acknowledgeMessages = acknowledgeMessages;
        this.acknowledgeMessage = acknowledgeMessage;
    }

    /**
     * the rest status that should be returned in the http response
     */
    @Override
    public RestStatus status() {
        return status.getRestStatus();
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {

        builder.field(TRIAL_WAS_STARTED_FIELD.getPreferredName(), status.isTrialStarted());
        builder.field(TRIAL_STATUS_FIELD.getPreferredName(), status.toString());
        builder.field(LICENSE_TYPE_FIELD.getPreferredName(), licenseType);

        if (status.getErrorMessage() != null) {
            builder.field(ERROR_MESSAGE_FIELD.getPreferredName(), status.getErrorMessage());
        }

        if (acknowledgeMessages.isEmpty() == false) {
            builder.startObject(ACKNOWLEDGE_DETAILS_FIELD.getPreferredName());
            builder.field(ACKNOWLEDGE_HEADER_FIELD.getPreferredName(), acknowledgeMessage);
            for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
                final String feature = entry.getKey();
                final String[] messages = entry.getValue();
                builder.array(feature, messages);
            }
            builder.endObject();
        }
    }

    public static PostStartTrialResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            // this class was formerly a direct subclass of ActionResponse
            super.readFrom(in);
            licenseType = in.readString();
        }

        status = in.readEnum(Status.class);

        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
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
        } else {
            this.acknowledgeMessages = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            // this class was formerly a direct subclass of ActionResponse
            super.writeTo(out);
            out.writeString(licenseType);
        }

        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
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
        } else {
            if (status == Status.UPGRADED_TO_TRIAL) {
                out.writeEnum(Pre63Status.UPGRADED_TO_TRIAL);
            } else if (status == Status.TRIAL_ALREADY_ACTIVATED) {
                out.writeEnum(Pre63Status.TRIAL_ALREADY_ACTIVATED);
            } else {
                throw new IllegalArgumentException("Starting trial on node with version [" + Version.CURRENT + "] requires " +
                        "acknowledgement parameter.");
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        PostStartTrialResponse otherResponse = (PostStartTrialResponse) other;
        return super.equals(otherResponse)
            && Objects.equals(otherResponse.getStatus(), getStatus())
            && Objects.equals(otherResponse.isAcknowledged(), isAcknowledged())
            && Objects.equals(otherResponse.getLicenseType(), getLicenseType())
            && Objects.equals(otherResponse.getAcknowledgementMessage(), getAcknowledgementMessage())
            && ProtocolUtils.equals(otherResponse.getAcknowledgementMessages(), getAcknowledgementMessages());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            getStatus(),
            isAcknowledged(),
            getLicenseType(),
            getAcknowledgementMessage(),
            ProtocolUtils.hashCode(getAcknowledgementMessages()));
    }


    /**
     * this object's internal representation of what state the trial is in
     */
    Status getStatus() {
        return status;
    }

    String getLicenseType() {
        return licenseType;
    }

    Map<String, String[]> getAcknowledgementMessages() {
        return acknowledgeMessages;
    }

    String getAcknowledgementMessage() {
        return acknowledgeMessage;
    }
}
