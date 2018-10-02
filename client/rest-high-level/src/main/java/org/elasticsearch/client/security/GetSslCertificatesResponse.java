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

import org.elasticsearch.client.security.support.CertificateInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        List<Object> unparsedCerts = parser.list();
        if (unparsedCerts.isEmpty()) {
            return new GetSslCertificatesResponse(Collections.emptyList());
        }
        List<CertificateInfo> certificates = new ArrayList<>();
        for (Object cert : unparsedCerts) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> value = (Map<String, ?>) cert;
                builder.map(value);
                try (XContentParser certificateInfoParser = XContentFactory.xContent(builder.contentType()).createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATION_HANDLER, Strings.toString(builder))) {
                    certificates.add(CertificateInfo.PARSER.parse(certificateInfoParser, null));
                }
            }
        }
        return new GetSslCertificatesResponse(certificates);
    }

    public List<CertificateInfo> getCertificates() {
        return certificates;
    }
}
