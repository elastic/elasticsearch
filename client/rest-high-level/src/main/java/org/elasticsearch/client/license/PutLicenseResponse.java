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
import org.elasticsearch.client.common.ProtocolUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class PutLicenseResponse {

    private static final ConstructingObjectParser<PutLicenseResponse, Void> PARSER = new ConstructingObjectParser<>(
        "put_license_response", true, (a, v) -> {
        boolean acknowledged = (Boolean) a[0];
        LicensesStatus licensesStatus = LicensesStatus.fromString((String) a[1]);
        @SuppressWarnings("unchecked") Tuple<String, Map<String, String[]>> acknowledgements = (Tuple<String, Map<String, String[]>>) a[2];
        if (acknowledgements == null) {
            return new PutLicenseResponse(acknowledged, licensesStatus);
        } else {
            return new PutLicenseResponse(acknowledged, licensesStatus, acknowledgements.v1(), acknowledgements.v2());
        }

    });

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("acknowledged"));
        PARSER.declareString(constructorArg(), new ParseField("license_status"));
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
            },
            new ParseField("acknowledge"));
    }

    private boolean acknowledged;
    private LicensesStatus status;
    private Map<String, String[]> acknowledgeMessages;
    private String acknowledgeHeader;

    public PutLicenseResponse() {
    }

    public PutLicenseResponse(boolean acknowledged, LicensesStatus status) {
        this(acknowledged, status, null, Collections.<String, String[]>emptyMap());
    }

    public PutLicenseResponse(boolean acknowledged, LicensesStatus status, String acknowledgeHeader,
                              Map<String, String[]> acknowledgeMessages) {
        this.acknowledged = acknowledged;
        this.status = status;
        this.acknowledgeHeader = acknowledgeHeader;
        this.acknowledgeMessages = acknowledgeMessages;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public LicensesStatus status() {
        return status;
    }

    public Map<String, String[]> acknowledgeMessages() {
        return acknowledgeMessages;
    }

    public String acknowledgeHeader() {
        return acknowledgeHeader;
    }

    public static PutLicenseResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PutLicenseResponse that = (PutLicenseResponse) o;

        return status == that.status &&
            ProtocolUtils.equals(acknowledgeMessages, that.acknowledgeMessages) &&
            Objects.equals(acknowledgeHeader, that.acknowledgeHeader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), status, ProtocolUtils.hashCode(acknowledgeMessages), acknowledgeHeader);
    }

}
