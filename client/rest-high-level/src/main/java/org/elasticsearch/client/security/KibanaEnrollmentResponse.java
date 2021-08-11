/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public final class KibanaEnrollmentResponse {

    private SecureString token;
    private String httpCa;

    public KibanaEnrollmentResponse(SecureString token, String httpCa) {
        this.token = token;
        this.httpCa = httpCa;
    }

    public SecureString getToken() { return token; }

    public String getHttpCa() {
        return httpCa;
    }

    private static final ParseField TOKEN = new ParseField("token");
    private static final ParseField HTTP_CA = new ParseField("http_ca");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<KibanaEnrollmentResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            KibanaEnrollmentResponse.class.getName(), true,
            a -> new KibanaEnrollmentResponse(new SecureString(((String) a[0]).toCharArray()), (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TOKEN);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA);
    }

    public static KibanaEnrollmentResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KibanaEnrollmentResponse that = (KibanaEnrollmentResponse) o;
        return token.equals(that.token) && httpCa.equals(that.httpCa);
    }

    @Override public int hashCode() {
        return Objects.hash(token, httpCa);
    }
}
