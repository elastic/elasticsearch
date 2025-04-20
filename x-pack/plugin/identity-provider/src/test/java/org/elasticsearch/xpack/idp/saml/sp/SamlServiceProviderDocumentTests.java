/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.security.x509.X509Credential;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SamlServiceProviderDocumentTests extends IdpSamlTestCase {

    public void testValidationFailuresForMissingFields() throws Exception {
        final SamlServiceProviderDocument doc = new SamlServiceProviderDocument();
        doc.setDocId(randomAlphaOfLength(16));
        final ValidationException validationException = doc.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.validationErrors(), not(emptyIterable()));
        assertThat(
            validationException.validationErrors(),
            Matchers.containsInAnyOrder(
                "field [name] is required, but was [null]",
                "field [entity_id] is required, but was [null]",
                "field [acs] is required, but was [null]",
                "field [created] is required, but was [null]",
                "field [last_modified] is required, but was [null]",
                "field [privileges.resource] is required, but was [null]",
                "field [attributes.principal] is required, but was [null]"
            )
        );
    }

    public void testValidationSucceedsWithMinimalFields() throws Exception {
        final SamlServiceProviderDocument doc = createMinimalDocument();
        final ValidationException validationException = doc.validate();
        assertThat(validationException, nullValue());
    }

    public void testXContentRoundTripWithMinimalFields() throws Exception {
        final SamlServiceProviderDocument doc1 = createMinimalDocument();
        final SamlServiceProviderDocument doc2 = assertXContentRoundTrip(doc1);
        assertThat(assertXContentRoundTrip(doc2), equalTo(doc1));
    }

    public void testXContentRoundTripWithAllFields() throws Exception {
        final SamlServiceProviderDocument doc1 = createFullDocument();
        final SamlServiceProviderDocument doc2 = assertXContentRoundTrip(doc1);
        assertThat(assertXContentRoundTrip(doc2), equalTo(doc1));
    }

    public void testStreamRoundTripWithMinimalFields() throws Exception {
        final SamlServiceProviderDocument doc1 = createMinimalDocument();
        final SamlServiceProviderDocument doc2 = assertXContentRoundTrip(doc1);
        assertThat(assertSerializationRoundTrip(doc2), equalTo(doc1));
    }

    public void testStreamRoundTripWithAllFields() throws Exception {
        final SamlServiceProviderDocument doc1 = createFullDocument();
        final SamlServiceProviderDocument doc2 = assertXContentRoundTrip(doc1);
        assertThat(assertSerializationRoundTrip(doc2), equalTo(doc1));
    }

    private SamlServiceProviderDocument createFullDocument() throws GeneralSecurityException, IOException {
        final List<X509Credential> credentials = readCredentials();
        final List<X509Certificate> certificates = credentials.stream().map(X509Credential::getEntityCertificate).toList();
        final List<X509Certificate> spCertificates = randomSubsetOf(certificates);
        final List<X509Certificate> idpCertificates = randomSubsetOf(certificates);
        final List<X509Certificate> idpMetadataCertificates = randomSubsetOf(certificates);

        final SamlServiceProviderDocument doc1 = new SamlServiceProviderDocument();
        doc1.setDocId(randomAlphaOfLength(16));
        doc1.setName(randomAlphaOfLengthBetween(8, 12));
        doc1.setEntityId("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        doc1.setAcs("https://" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8) + "/saml/acs");
        doc1.setCreatedMillis(System.currentTimeMillis() - randomIntBetween(100_000, 1_000_000));
        doc1.setLastModifiedMillis(System.currentTimeMillis() - randomIntBetween(1_000, 100_000));
        doc1.setNameIdFormat(randomFrom(NameID.TRANSIENT, NameID.PERSISTENT, NameID.EMAIL));
        doc1.setAuthenticationExpiryMillis(randomLongBetween(100, 5_000_000));
        doc1.certificates.setServiceProviderX509SigningCertificates(spCertificates);
        doc1.certificates.setIdentityProviderX509SigningCertificates(idpCertificates);
        doc1.certificates.setIdentityProviderX509MetadataSigningCertificates(idpMetadataCertificates);

        doc1.privileges.setResource("service:" + randomAlphaOfLength(12) + ":" + randomAlphaOfLength(12));
        final Set<String> rolePatterns = new HashSet<>();
        for (int i = randomIntBetween(1, 6); i > 0; i--) {
            rolePatterns.add(randomAlphaOfLength(6) + ":(" + randomAlphaOfLength(6) + ")");
        }
        doc1.privileges.setRolePatterns(rolePatterns);

        doc1.attributeNames.setPrincipal("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        doc1.attributeNames.setEmail("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        doc1.attributeNames.setName("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        doc1.attributeNames.setRoles("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        return doc1;
    }

    private SamlServiceProviderDocument createMinimalDocument() {
        final SamlServiceProviderDocument doc1 = new SamlServiceProviderDocument();
        doc1.setDocId(randomAlphaOfLength(16));
        doc1.setName(randomAlphaOfLengthBetween(8, 12));
        doc1.setEntityId("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        doc1.setAcs("https://" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8) + "/saml/acs");
        doc1.setCreatedMillis(System.currentTimeMillis() - randomIntBetween(100_000, 1_000_000));
        doc1.setLastModifiedMillis(System.currentTimeMillis() - randomIntBetween(1_000, 100_000));
        doc1.privileges.setResource("service:" + randomAlphaOfLength(12) + ":" + randomAlphaOfLength(12));
        doc1.attributeNames.setPrincipal("urn:" + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(4, 8));
        return doc1;
    }

    private SamlServiceProviderDocument assertXContentRoundTrip(SamlServiceProviderDocument obj1) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();
        final BytesReference bytes1 = XContentHelper.toXContent(obj1, xContentType, humanReadable);
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes1,
                xContentType
            )
        ) {
            final SamlServiceProviderDocument obj2 = SamlServiceProviderDocument.fromXContent(obj1.docId, parser);
            assertThat(obj2, equalTo(obj1));

            final BytesReference bytes2 = XContentHelper.toXContent(obj2, xContentType, humanReadable);
            assertToXContentEquivalent(bytes1, bytes2, xContentType);

            return obj2;
        }
    }

    private SamlServiceProviderDocument assertSerializationRoundTrip(SamlServiceProviderDocument doc) throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_0_0,
            TransportVersion.current()
        );
        final SamlServiceProviderDocument read = copyWriteable(
            doc,
            new NamedWriteableRegistry(List.of()),
            SamlServiceProviderDocument::new,
            version
        );
        MatcherAssert.assertThat("Serialized document with version [" + version + "] does not match original object", read, equalTo(doc));
        return read;
    }

}
