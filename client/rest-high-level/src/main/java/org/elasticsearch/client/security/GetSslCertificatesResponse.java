/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.CertificateInfo;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Response object when retrieving the X.509 certificates that are used to encrypt communications in an Elasticsearch cluster.
 * Returns a list of {@link CertificateInfo} objects describing each of the certificates.
 */
public final class GetSslCertificatesResponse {

    private final List<CertificateInfo> certificates;

    public GetSslCertificatesResponse(List<CertificateInfo> certificates) {
        this.certificates = certificates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetSslCertificatesResponse that = (GetSslCertificatesResponse) o;
        return Objects.equals(this.certificates, that.certificates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(certificates);
    }

    public static GetSslCertificatesResponse fromXContent(XContentParser parser) throws IOException {
        List<CertificateInfo> certificates = new ArrayList<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            certificates.add(CertificateInfo.PARSER.parse(parser, null));
        }
        return new GetSslCertificatesResponse(certificates);
    }

    public List<CertificateInfo> getCertificates() {
        return certificates;
    }
}
