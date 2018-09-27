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

package org.elasticsearch.client.security;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetSslCertificatesResponse {

    private final String path;
    private final String format;
    private final String alias;
    private final String subjectDn;
    private final String serialNumber;
    private final String expiry;
    private final boolean hasPrivateKey;

    public GetSslCertificatesResponse(String path, String format, @Nullable String alias, String subjectDn, String serialNumber, String expiry,
                                      boolean hasPrivateKey) {
        this.path = path;
        this.format = format;
        this.alias = alias;
        this.subjectDn = subjectDn;
        this.serialNumber = serialNumber;
        this.expiry = expiry;
        this.hasPrivateKey = hasPrivateKey;
    }

    /**
     * @return The path to the certificate, as configured in the elasticsearch.yml file.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return The format of the file. One of jks, PKCS12, PEM
     */
    public String getFormat() {
        return format;
    }

    public @Nullable
    String getAlias() {
        return alias;
    }

    public String getSubjectDn() {
        return subjectDn;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public String getExpiry() {
        return expiry;
    }

    public boolean isHasPrivateKey() {
        return hasPrivateKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetSslCertificatesResponse that = (GetSslCertificatesResponse) o;
        return path.equals(that.path) &&
            format.equals(that.format) &&
            Objects.equals(alias, that.alias) &&
            serialNumber.equals(that.serialNumber) &&
            expiry.equals(that.expiry) &&
            hasPrivateKey == that.hasPrivateKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, format, alias, serialNumber, expiry, hasPrivateKey);
    }

    private static final ConstructingObjectParser<GetSslCertificatesResponse, Void> PARSER = new ConstructingObjectParser<>
        ("get_ssl_certificates_response",
            true, args -> new GetSslCertificatesResponse((String) args[0], (String) args[1], (String) args[2], (String) args[3],
            (String) args[4], (String) args[5], (boolean) args[6]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("path"));
        PARSER.declareString(constructorArg(), new ParseField("format"));
        PARSER.declareStringOrNull(constructorArg(), new ParseField("alias"));
        PARSER.declareString(constructorArg(), new ParseField("subject_dn"));
        PARSER.declareString(constructorArg(), new ParseField("serial_number"));
        PARSER.declareString(constructorArg(), new ParseField("expiry"));
        PARSER.declareBoolean(constructorArg(), new ParseField("has_private_key"));
    }

    public static GetSslCertificatesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
