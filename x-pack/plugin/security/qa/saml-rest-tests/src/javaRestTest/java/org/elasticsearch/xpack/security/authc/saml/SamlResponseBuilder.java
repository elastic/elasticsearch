/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.schema.XSString;
import org.opensaml.core.xml.schema.impl.XSStringBuilder;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.AttributeValue;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnContext;
import org.opensaml.saml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml.saml2.core.AuthnStatement;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;
import org.opensaml.saml.saml2.core.impl.AuthnStatementBuilder;
import org.opensaml.security.credential.BasicCredential;
import org.opensaml.security.credential.Credential;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.Signer;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivilegedActionException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE;
import static org.opensaml.saml.saml2.core.AuthnContext.PASSWORD_AUTHN_CTX;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_BEARER;

/**
 * This class is designed to support building SAML authentication "Response" objects for test scenarios.
 * It currently exists in a specific SAML project but could be moved to a general testing location is necessary
 */
public class SamlResponseBuilder {

    private static final Logger logger = LogManager.getLogger(SamlResponseBuilder.class);

    private String id;
    private String inResponseTo;
    private String sessionIndex;
    private String nameId;
    private Instant now;
    private Instant validUntil;

    private String spEntityId;
    private String acs;

    private String idpEntityId;

    private Map<String, List<String>> attributes;

    private CheckedConsumer<Signature, GeneralSecurityException> signer;

    public SamlResponseBuilder() {
        try {
            SamlUtils.initialize(logger);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Cannot initialise SAML utilities", e);
        }

        this.attributes = new HashMap<>();
        this.id = SamlUtils.generateSecureNCName(24);
        this.inResponseTo = null;
        this.sessionIndex = SamlUtils.generateSecureNCName(12);
        this.nameId = SamlUtils.generateSecureNCName(36);
        this.now = Instant.now();
        this.validUntil = now.plus(1, ChronoUnit.HOURS);
    }

    public SamlResponseBuilder spEntityId(String id) {
        this.spEntityId = id;
        return this;
    }

    public SamlResponseBuilder acs(URL acs) {
        return acs(acs.toString());
    }

    private SamlResponseBuilder acs(String acs) {
        this.acs = acs;
        return this;
    }

    public SamlResponseBuilder idpEntityId(String id) {
        idpEntityId = id;
        return this;
    }

    public SamlResponseBuilder attribute(String name, String value) {
        this.attributes.put(name, List.of(value));
        return this;
    }

    public SamlResponseBuilder sign(Path certPath, Path keyPath, char[] keyPassword) throws GeneralSecurityException, IOException {
        var privateKey = PemUtils.readPrivateKey(keyPath, () -> keyPassword);
        var certificates = PemUtils.readCertificates(List.of(certPath));
        if (certificates.size() != 1) {
            throw new IllegalArgumentException(
                "Expected to find exactly one certificate in " + certPath.toAbsolutePath() + " but found " + certificates.size()
            );
        }
        var certificate = certificates.get(0);
        if (certificate instanceof X509Certificate == false) {
            throw new IllegalArgumentException(
                "Expected certificate "
                    + certificate
                    + " from "
                    + certPath.toAbsolutePath()
                    + " to be of type "
                    + X509Certificate.class.getName()
            );
        }
        this.signer = signature -> {
            final Credential credential = new BasicCredential(certificate.getPublicKey(), privateKey);
            signature.setSigningCredential(credential);

            final org.opensaml.xmlsec.signature.KeyInfo keyInfo = SamlUtils.buildObject(
                org.opensaml.xmlsec.signature.KeyInfo.class,
                org.opensaml.xmlsec.signature.KeyInfo.DEFAULT_ELEMENT_NAME
            );
            KeyInfoSupport.addCertificate(keyInfo, (X509Certificate) certificate);
            signature.setKeyInfo(keyInfo);

            // TODO This assumes the private key is an RSA key
            signature.setSignatureAlgorithm("http://www.w3.org/2001/04/xmldsig-more#rsa-sha512");
        };
        return this;
    }

    public String asString() throws MarshallingException, GeneralSecurityException, SignatureException {
        return SamlUtils.getXmlContent(build(), false);
    }

    public Response build() throws MarshallingException, GeneralSecurityException, SignatureException {
        var response = buildResponse();
        if (signer != null) {
            sign(response);
        }
        return response;
    }

    private void sign(Response response) throws GeneralSecurityException, MarshallingException, SignatureException {
        final Signature signature = SamlUtils.buildObject(Signature.class, Signature.DEFAULT_ELEMENT_NAME);
        signature.setCanonicalizationAlgorithm(EXCLUSIVE);
        this.signer.accept(signature);
        response.setSignature(signature);
        XMLObjectProviderRegistrySupport.getMarshallerFactory().getMarshaller(response).marshall(response);
        Signer.signObject(signature);
    }

    private Response buildResponse() {
        final Response response = SamlUtils.buildObject(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setDestination(acs);
        response.setID(id);
        response.setInResponseTo(inResponseTo);
        response.setIssueInstant(now);
        final Issuer responseIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        responseIssuer.setValue(idpEntityId);
        response.setIssuer(responseIssuer);
        final Status status = SamlUtils.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        final StatusCode statusCode = SamlUtils.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(StatusCode.SUCCESS);
        status.setStatusCode(statusCode);
        response.setStatus(status);

        final Assertion assertion = SamlUtils.buildObject(Assertion.class, Assertion.DEFAULT_ELEMENT_NAME);
        assertion.setID(sessionIndex);
        assertion.setIssueInstant(now);
        final Issuer assertionIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        assertionIssuer.setValue(idpEntityId);
        assertion.setIssuer(assertionIssuer);
        AudienceRestriction audienceRestriction = SamlUtils.buildObject(
            AudienceRestriction.class,
            AudienceRestriction.DEFAULT_ELEMENT_NAME
        );
        Audience audience = SamlUtils.buildObject(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        audience.setURI(spEntityId);
        audienceRestriction.getAudiences().add(audience);
        Conditions conditions = SamlUtils.buildObject(Conditions.class, Conditions.DEFAULT_ELEMENT_NAME);
        conditions.getAudienceRestrictions().add(audienceRestriction);
        assertion.setConditions(conditions);
        final Subject subject = SamlUtils.buildObject(Subject.class, Subject.DEFAULT_ELEMENT_NAME);
        final NameID nameIDElement = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameIDElement.setFormat(TRANSIENT);
        nameIDElement.setNameQualifier(idpEntityId);
        nameIDElement.setSPNameQualifier(spEntityId);
        nameIDElement.setValue(nameId);
        final SubjectConfirmation subjectConfirmation = SamlUtils.buildObject(
            SubjectConfirmation.class,
            SubjectConfirmation.DEFAULT_ELEMENT_NAME
        );
        final SubjectConfirmationData subjectConfirmationData = SamlUtils.buildObject(
            SubjectConfirmationData.class,
            SubjectConfirmationData.DEFAULT_ELEMENT_NAME
        );
        subjectConfirmationData.setNotOnOrAfter(validUntil);
        subjectConfirmationData.setRecipient(acs);
        subjectConfirmationData.setInResponseTo(inResponseTo);
        subjectConfirmation.setSubjectConfirmationData(subjectConfirmationData);
        subjectConfirmation.setMethod(METHOD_BEARER);
        subject.setNameID(nameIDElement);
        subject.getSubjectConfirmations().add(subjectConfirmation);
        assertion.setSubject(subject);
        final AuthnContextClassRef authnContextClassRef = SamlUtils.buildObject(
            AuthnContextClassRef.class,
            AuthnContextClassRef.DEFAULT_ELEMENT_NAME
        );
        authnContextClassRef.setURI(PASSWORD_AUTHN_CTX);
        final AuthnContext authnContext = SamlUtils.buildObject(AuthnContext.class, AuthnContext.DEFAULT_ELEMENT_NAME);
        authnContext.setAuthnContextClassRef(authnContextClassRef);
        final AuthnStatement authnStatement = new AuthnStatementBuilder().buildObject();
        authnStatement.setAuthnContext(authnContext);
        authnStatement.setAuthnInstant(now);
        authnStatement.setSessionIndex(sessionIndex);
        authnStatement.setSessionNotOnOrAfter(validUntil);
        assertion.getAuthnStatements().add(authnStatement);

        final AttributeStatement attributeStatement = SamlUtils.buildObject(
            AttributeStatement.class,
            AttributeStatement.DEFAULT_ELEMENT_NAME
        );
        this.attributes.forEach((attributeName, attributeValues) -> {
            final Attribute attribute = SamlUtils.buildObject(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
            attribute.setName(attributeName);
            attributeValues.forEach(value -> {
                XSStringBuilder stringBuilder = new XSStringBuilder();
                XSString stringValue = stringBuilder.buildObject(AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
                stringValue.setValue(value);
                attribute.getAttributeValues().add(stringValue);
            });
            attributeStatement.getAttributes().add(attribute);
        });
        assertion.getAttributeStatements().add(attributeStatement);

        response.getAssertions().add(assertion);

        return response;
    }

}
