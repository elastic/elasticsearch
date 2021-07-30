/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public final class KibanaEnrollmentResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField PASSWORD = new ParseField("password");
    private static final ParseField HTTP_CA = new ParseField("http_ca");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KibanaEnrollmentResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            KibanaEnrollmentResponse.class.getName(), true,
            a -> new KibanaEnrollmentResponse(new SecureString(((String) a[0]).toCharArray()), (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PASSWORD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA);
    }

    private final SecureString password;
    private final String httpCa;

    public KibanaEnrollmentResponse(StreamInput in) throws IOException {
        super(in);
        password = in.readSecureString();
        httpCa = in.readString();
    }

    public KibanaEnrollmentResponse(SecureString password, String httpCa) {
        this.password = password;
        this.httpCa = httpCa;
    }

    public SecureString getPassword() { return password; }
    public String getHttpCa() {
        return httpCa;
    }

    @Override public XContentBuilder toXContent(
        XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PASSWORD.getPreferredName(), password.toString());
        builder.field(HTTP_CA.getPreferredName(), httpCa);
        return builder.endObject();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(password);
        out.writeString(httpCa);
    }

    public static KibanaEnrollmentResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KibanaEnrollmentResponse response = (KibanaEnrollmentResponse) o;
        return password.equals(response.password) && httpCa.equals(response.httpCa);
    }

    @Override public int hashCode() {
        return Objects.hash(password, httpCa);
    }
}
