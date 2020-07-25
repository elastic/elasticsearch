/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Before;
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
import org.opensaml.saml.saml2.core.EncryptedAssertion;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;
import org.opensaml.saml.saml2.core.impl.AuthnStatementBuilder;
import org.opensaml.saml.saml2.encryption.Encrypter;
import org.opensaml.security.credential.BasicCredential;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.encryption.support.DataEncryptionParameters;
import org.opensaml.xmlsec.encryption.support.DecryptionException;
import org.opensaml.xmlsec.encryption.support.EncryptionConstants;
import org.opensaml.xmlsec.encryption.support.KeyEncryptionParameters;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE;
import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE_WITH_COMMENTS;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML20P_NS;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML20_NS;
import static org.opensaml.saml.saml2.core.AuthnContext.KERBEROS_AUTHN_CTX;
import static org.opensaml.saml.saml2.core.AuthnContext.PASSWORD_AUTHN_CTX;
import static org.opensaml.saml.saml2.core.AuthnContext.X509_AUTHN_CTX;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_BEARER;
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_HOLDER_OF_KEY;

public class SamlAuthenticatorTests extends SamlResponseHandlerTests {

    private static final String UID_OID = "urn:oid:0.9.2342.19200300.100.1.1";

    private SamlAuthenticator authenticator;

    @Before
    public void setupAuthenticator() throws Exception {
        this.clock = new ClockMock();
        this.maxSkew = TimeValue.timeValueMinutes(1);
        this.authenticator = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair), emptyList());
        this.requestId = randomId();
    }

    private SamlAuthenticator buildAuthenticator(Supplier<List<Credential>> credentials, List<String> reqAuthnCtxClassRef)
        throws Exception {
        final IdpConfiguration idp = new IdpConfiguration(IDP_ENTITY_ID, credentials);

        final SigningConfiguration signingConfiguration = new SigningConfiguration(Collections.singleton("*"),
            (X509Credential) buildOpenSamlCredential(spSigningCertificatePair).get(0));
        final List<X509Credential> spEncryptionCredentials = buildOpenSamlCredential(spEncryptionCertificatePairs).stream()
            .map((cred) -> (X509Credential) cred).collect(Collectors.<X509Credential>toList());
        final SpConfiguration sp = new SpConfiguration(SP_ENTITY_ID, SP_ACS_URL, null, signingConfiguration, spEncryptionCredentials,
            reqAuthnCtxClassRef);
        return new SamlAuthenticator(
            clock,
            idp,
            sp,
            maxSkew
        );
    }

    public void testParseEmptyContentIsRejected() throws Exception {
        SamlToken token = token("");
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Failed to parse"));
        assertThat(exception.getCause(), Matchers.instanceOf(SAXException.class));
    }

    public void testParseContentWithNoAssertionsIsRejected() throws Exception {
        final Instant now = clock.instant();
        final Response response = SamlUtils.buildObject(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setDestination(SP_ACS_URL);
        response.setID(randomId());
        response.setInResponseTo(requestId);
        response.setIssueInstant(new DateTime(now.toEpochMilli()));
        final Issuer responseIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        responseIssuer.setValue(IDP_ENTITY_ID);
        response.setIssuer(responseIssuer);
        final Status status = SamlUtils.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        final StatusCode statusCode = SamlUtils.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(StatusCode.SUCCESS);
        status.setStatusCode(statusCode);
        response.setStatus(status);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(xml);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("No assertions found in SAML response"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testSuccessfullyParseContentWithASingleValidAssertion() throws Exception {
        Instant now = clock.instant();
        final String nameId = randomAlphaOfLengthBetween(12, 24);
        final String sessionindex = randomId();
        final String xml = getSimpleResponseAsString(now, nameId, sessionindex);

        SamlToken token = token(signResponse(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        assertThat(uid, iterableWithSize(1));
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(nameId));
    }

    public void testSuccessfullyParseContentFromRawXmlWithASingleValidAssertion() throws Exception {
        Instant now = clock.instant();
        final String nameId = randomAlphaOfLengthBetween(12, 24);
        final String sessionindex = randomId();
        final String xml = getSimpleResponseFromXmlTemplate(now, nameId, sessionindex);

        SamlToken token = token(signResponse(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        assertThat(uid, iterableWithSize(1));
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(nameId));
    }

    public void testSuccessfullyParseContentWithMultipleValidAttributes() throws Exception {
        final String nameId = randomAlphaOfLengthBetween(4, 8) + "-" + randomAlphaOfLengthBetween(8, 12);
        final String session = randomId();

        final String xml = getSimpleResponseAsString(clock.instant(), nameId, session);
        SamlToken token = token(signResponse(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));

        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(nameId));
        assertThat(attributes.name().idpNameQualifier, equalTo(IDP_ENTITY_ID));
        assertThat(attributes.name().spNameQualifier, equalTo(SP_ENTITY_ID));

        assertThat(attributes.session(), equalTo(session));
    }

    public void testSuccessfullyParseContentFromEncryptedAssertion() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);

        final Response encrypted = encryptAssertions(xml, randomFrom(spEncryptionCertificatePairs));
        final String encryptedString = SamlUtils.getXmlContent(encrypted, false);
        assertThat(encryptedString, not(equalTo(xml)));

        final String signed = signResponse(encryptedString);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testSuccessfullyParseContentFromEncryptedAndSignedAssertion() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final String signed = signAssertions(xml);
        assertThat(signed, not(equalTo(xml)));
        final Response encrypted = encryptAssertions(signed, randomFrom(spEncryptionCertificatePairs));
        final String encryptedString = SamlUtils.getXmlContent(encrypted, false);
        assertThat(encryptedString, not(equalTo(signed)));

        final SamlToken token = token(encryptedString);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testSuccessfullyParseContentFromEncryptedAttribute() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        final Instant now = clock.instant();
        String xml = getSimpleResponseAsString(now);
        /**
         * This hack is necessary because if we leave the NS declaration as prefixed with saml2, the parser will
         * remove it as redundant when marshalling the Response object ( as it is already declared in the Assertion element)
         * This would have the side effect that during decryption, when {@link org.opensaml.saml.saml2.encryption.Decrypter}
         * would decrypt the EncryptedAttribute, there would be no NS declaration for saml2 and parsing would fail with
         * org.xml.sax.SAXParseException: The prefix "saml2" for element "saml2:Attribute" is not bound.
         */
        xml = xml.replace("<saml2:Attribute ",
            "<Attribute xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\" ")
            .replace("</saml2:Attribute>", "</Attribute>")
            .replace("<saml2:AttributeValue ",
                "<AttributeValue xmlns=\"urn:oasis:names:tc:SAML:2.0:assertion\" ")
            .replace("</saml2:AttributeValue>", "</AttributeValue>");
        final Response encrypted = encryptAttributes(xml, randomFrom(spEncryptionCertificatePairs));
        String encryptedString = SamlUtils.getXmlContent(encrypted, false);
        assertThat(encryptedString, not(equalTo(xml)));
        final String signed = signer.transform(encryptedString, idpSigningCertificatePair);
        assertThat(signed, not(equalTo(encryptedString)));

        final SamlToken token = token(signed);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testFailWhenAssertionsCannotBeDecrypted() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);

        final Response encrypted = encryptAssertions(xml, readKeyPair("ENCRYPTION_RSA_4096_updated"));
        final String encryptedString = SamlUtils.getXmlContent(encrypted, false);
        assertThat(encryptedString, not(equalTo(xml)));

        final String signed = signResponse(encrypted);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Failed to decrypt"));
        assertThat(exception.getCause(), instanceOf(DecryptionException.class));
    }

    public void testNoAttributesReturnedWhenTheyCannotBeDecrypted() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);

        // Encrypting with different cert instead of sp cert will mean that the SP cannot decrypt
        final Response encrypted = encryptAttributes(xml, readKeyPair("ENCRYPTION_RSA_4096_updated"));
        final String encryptedString = SamlUtils.getXmlContent(encrypted, false);
        assertThat(encryptedString, not(equalTo(xml)));

        final String signed = signResponse(encrypted);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        // Because an assertion can theoretically contains encrypted and unencrypted attributes
        // we don't treat a decryption as a hard failure.
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.attributes(), iterableWithSize(0));
    }

    public void testIncorrectResponseIssuerIsRejected() throws Exception {
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        final Issuer wrongIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        wrongIssuer.setValue("wrong_issuer");
        response.setIssuer(wrongIssuer);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Issuer"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectAssertionIssuerIsRejected() throws Exception {
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        final Issuer wrongIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        wrongIssuer.setValue("wrong_issuer");
        response.getAssertions().get(0).setIssuer(wrongIssuer);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Issuer"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectDestinationIsRejected() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        response.setDestination("invalid_destination");
        final String xml = SamlUtils.getXmlContent(response, false);
        final String signed = signer.transform(xml, idpSigningCertificatePair);
        SamlToken token = token(signed);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("destination"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testMissingDestinationIsNotRejectedForNotSignedResponse() throws Exception {
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        response.setDestination("");
        final String xml = SamlUtils.getXmlContent(response, false);

        SamlToken token = token(signAssertions(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        assertThat(uid, iterableWithSize(1));
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
    }

    public void testIncorrectRequestIdIsRejected() throws Exception {
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        response.setInResponseTo("someotherID");
        final String xml = SamlUtils.getXmlContent(response, false);

        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("in-response-to"));
        assertThat(exception.getMessage(), containsString(requestId));
        assertThat(exception.getMessage(), containsString("someotherID"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectRecipientIsRejected() throws Exception {
        Instant now = clock.instant();
        final Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getSubject().getSubjectConfirmations().get(0).getSubjectConfirmationData()
            .setRecipient(SP_ACS_URL+"/fake");
        final String xml = SamlUtils.getXmlContent(response, false);

        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion SubjectConfirmationData Recipient"));
        assertThat(exception.getMessage(), containsString(SP_ACS_URL + "/fake"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).setSubject(null);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("has no Subject"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutAuthnStatementIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getAuthnStatements().clear();
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Authn Statements while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testExpiredAuthnStatementSessionIsRejected() throws Exception {
        Instant now = clock.instant();
        String xml = getSimpleResponseAsString(now);
        SamlToken token = token(signResponse(xml));
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance partway through the session expiry time
        clock.fastForwardSeconds(30);
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance past the expiry time, but allow for clock skew
        clock.fastForwardSeconds((int) (30 + maxSkew.seconds() / 2));
        assertThat(authenticator.authenticate(token), notNullValue());

        // but fails once we get past the clock skew allowance
        clock.fastForwardSeconds((int) (1 + maxSkew.seconds() / 2));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("on/after"));
        assertThat(exception.getMessage(), containsString("Authentication Statement"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectAuthnContextClassRefIsRejected() throws Exception {
        Instant now = clock.instant();
        String xml = getSimpleResponseAsString(now);

        SamlAuthenticator authenticatorWithReqAuthnCtx = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair),
            Arrays.asList(X509_AUTHN_CTX, KERBEROS_AUTHN_CTX));
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticatorWithReqAuthnCtx.authenticate(token));
        assertThat(exception.getMessage(), containsString("Rejecting SAML assertion as the AuthnContextClassRef"));
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectConfirmationIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getSubject().getSubjectConfirmations().clear();
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion subject contains [0] bearer SubjectConfirmation"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectConfirmationDataIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getSubject().getSubjectConfirmations().get(0).setSubjectConfirmationData(null);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("bearer SubjectConfirmation, while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssetionWithoutBearerSubjectConfirmationMethodIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getSubject().getSubjectConfirmations().get(0).setMethod(METHOD_HOLDER_OF_KEY);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("bearer SubjectConfirmation, while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectSubjectConfirmationDataInResponseToIsRejected() throws Exception {
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.getAssertions().get(0).getSubject().getSubjectConfirmations().get(0).getSubjectConfirmationData().setInResponseTo(
            "incorrectId");
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion SubjectConfirmationData is in-response-to"));
        assertThat(exception.getMessage(), containsString(requestId));
        assertThat(exception.getMessage(), containsString("incorrectId"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testExpiredSubjectConfirmationDataIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(120);
        Response response = getSimpleResponse(now, randomId(), randomId(), validUntil, validUntil);

        // check that the content is valid "now"
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance partway through the expiry time
        clock.fastForwardSeconds(90);
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance past the expiry time, but allow for clock skew
        clock.fastForwardSeconds((int) (30 + maxSkew.seconds() / 2));
        assertThat(authenticator.authenticate(token), notNullValue());

        // but fails once we get past the clock skew allowance
        clock.fastForwardSeconds((int) (1 + maxSkew.seconds() / 2));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("on/after"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIdpInitiatedLoginIsAllowed() throws Exception {
        /* An IdP initiated login has no "in response to"
         * This might happen if:
         *  - The IDP has a list of services to pick from (like the Okta dashboard)
         *  - The IDP had to do some housework (like a forced password change) during the login flow, and switch from an in-response-to
         *    login to an IDP initiated login.
         */
        Instant now = clock.instant();
        Response response = getSimpleResponse(now);
        response.setInResponseTo(null);
        final String xml = SamlUtils.getXmlContent(response, false);
        SamlToken token = token(signResponse(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
    }

    public void testIncorrectSigningKeyIsRejected() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);

        // check that the content is valid when signed by the correct key-pair
        assertThat(authenticator.authenticate(token(signer.transform(xml, idpSigningCertificatePair))), notNullValue());

        // check is rejected when signed by a different key-pair
        final Tuple<X509Certificate, PrivateKey> wrongKey = readKeyPair("RSA_4096_updated");
        final ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticator.authenticate(token(signer.transform(xml, wrongKey)))
        );
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testSigningKeyIsReloadedForEachRequest() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        final String xml = getSimpleResponseAsString(Instant.now());

        assertThat(authenticator.authenticate(token(signer.transform(xml, idpSigningCertificatePair))), notNullValue());

        final Tuple<X509Certificate, PrivateKey> oldKeyPair = idpSigningCertificatePair;
        // Ensure we won't read any of the ones we could have picked randomly before
        idpSigningCertificatePair = readKeyPair("RSA_4096_updated");
        assertThat(idpSigningCertificatePair.v2(), not(equalTo(oldKeyPair.v2())));
        assertThat(authenticator.authenticate(token(signer.transform(xml, idpSigningCertificatePair))), notNullValue());
        // Restore the keypair to one from the keypair pool of all algorithms and keys
        idpSigningCertificatePair = readRandomKeyPair(randomSigningAlgorithm());
    }

    public void testParsingRejectsTamperedContent() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        final String xml = getSimpleResponseAsString(Instant.now());

        // check that the original signed content is valid
        final String signed = signer.transform(xml, idpSigningCertificatePair);
        assertThat(authenticator.authenticate(token(signed)), notNullValue());

        // but altered content is rejected
        final String altered = signed.replace("daredevil", "iron fist");
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token(altered)));
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testSigningWhenIdpHasMultipleKeys() throws Exception {
        final int numberOfKeys = scaledRandomIntBetween(2, 6);
        final List<Tuple<X509Certificate, PrivateKey>> keys = new ArrayList<>(numberOfKeys);
        final List<Credential> credentials = new ArrayList<>(numberOfKeys);
        for (int i = 0; i < numberOfKeys; i++) {
            final Tuple<X509Certificate, PrivateKey> key = readRandomKeyPair(randomSigningAlgorithm());
            keys.add(key);
            credentials.addAll(buildOpenSamlCredential(key));
        }
        this.authenticator = buildAuthenticator(() -> credentials, emptyList());
        final CryptoTransform signer = randomBoolean() ? this::signResponse : this::signAssertions;
        final String xml = getSimpleResponseAsString(Instant.now());

        // check that the content is valid when signed by the each of the key-pairs
        for (Tuple<X509Certificate, PrivateKey> key : keys) {
            assertThat(authenticator.authenticate(token(signer.transform(xml, key))), notNullValue());
        }
    }

    public void testExpiredContentIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(120);
        final String xml = SamlUtils.getXmlContent(getSimpleResponse(now, randomId(), randomId(), validUntil, validUntil), false);
        // check that the content is valid "now"
        final SamlToken token = token(signResponse(xml));
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance partway through the expiry time
        clock.fastForwardSeconds(90);
        assertThat(authenticator.authenticate(token), notNullValue());

        // and still valid if we advance past the expiry time, but allow for clock skew
        clock.fastForwardSeconds((int) (30 + maxSkew.seconds() / 2));
        assertThat(authenticator.authenticate(token), notNullValue());

        // but fails once we get past the clock skew allowance
        clock.fastForwardSeconds((int) (1 + maxSkew.seconds() / 2));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("on/after"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testContentIsRejectedIfRestrictedToADifferentAudience() throws Exception {
        final String audience = "https://some.other.sp/SAML2";
        final Response response = getSimpleResponse(Instant.now());
        AudienceRestriction audienceRestriction = SamlUtils.buildObject(AudienceRestriction.class,
            AudienceRestriction.DEFAULT_ELEMENT_NAME);
        Audience falseAudience = SamlUtils.buildObject(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        falseAudience.setAudienceURI(audience);
        audienceRestriction.getAudiences().add(falseAudience);
        response.getAssertions().get(0).getConditions().getAudienceRestrictions().clear();
        response.getAssertions().get(0).getConditions().getAudienceRestrictions().add(audienceRestriction);
        String xml = SamlUtils.getXmlContent(response, false);
        final SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("required audience"));
        assertThat(exception.getMessage(), containsString(audience));
        assertThat(exception.getMessage(), containsString(SP_ENTITY_ID));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testLoggingWhenAudienceCheckFails() throws Exception {
        final String similarAudienceString = SP_ENTITY_ID.replaceFirst("/$", ":80/");
        final String wrongAudienceString = "http://" + randomAlphaOfLengthBetween(4, 12) + "." + randomAlphaOfLengthBetween(6, 8) + "/";
        final Response response = getSimpleResponse(Instant.now());
        AudienceRestriction invalidAudienceRestriction = SamlUtils.buildObject(AudienceRestriction.class,
            AudienceRestriction.DEFAULT_ELEMENT_NAME);
        Audience similarAudience = SamlUtils.buildObject(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        similarAudience.setAudienceURI(similarAudienceString);
        Audience wrongAudience = SamlUtils.buildObject(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        wrongAudience.setAudienceURI(wrongAudienceString);
        invalidAudienceRestriction.getAudiences().add(similarAudience);
        invalidAudienceRestriction.getAudiences().add(wrongAudience);
        response.getAssertions().get(0).getConditions().getAudienceRestrictions().clear();
        response.getAssertions().get(0).getConditions().getAudienceRestrictions().add(invalidAudienceRestriction);
        String xml = SamlUtils.getXmlContent(response, false);
        final SamlToken token = token(signResponse(xml));

        final Logger samlLogger = LogManager.getLogger(authenticator.getClass());
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(samlLogger, mockAppender);

            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "similar audience",
                authenticator.getClass().getName(),
                Level.INFO,
                "Audience restriction [" + similarAudienceString + "] does not match required audience [" + SP_ENTITY_ID +
                    "] (difference starts at character [#" + (SP_ENTITY_ID.length() - 1) + "] [:80/] vs [/])"
            ));
            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "not similar audience",
                authenticator.getClass().getName(),
                Level.INFO,
                "Audience restriction [" + wrongAudienceString + "] does not match required audience [" + SP_ENTITY_ID + "]"
            ));
            final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
            assertThat(exception.getMessage(), containsString("required audience"));
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(samlLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testContentIsRejectedIfNotMarkedAsSuccess() throws Exception {
        final String xml = getStatusFailedResponse();
        final SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("not a 'success' response"));
        assertThat(exception.getMessage(), containsString(StatusCode.REQUESTER));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    /*
     * Implement most of the attacks described in https://www.usenix.org/system/files/conference/usenixsecurity12/sec12-final91-8-23-12.pdf
     * as tests
     */

    public void testSignatureWrappingAttackOne() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signResponse(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 1 - Mangle the contents of the response to be
           <ForgedResponse>
               <LegitimateResponseSignature>
                   <LegitimateResponse></LegitimateResponse>
               </LegitimateResponseSignature>
               <ForgedAssertion></ForgedAssertion>
           </ForgedResponse>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element clonedResponse = (Element) response.cloneNode(true);
        final Element clonedSignature = (Element) clonedResponse.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        clonedResponse.removeChild(clonedSignature);
        final Element legitimateSignature = (Element) response.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        legitimateSignature.appendChild(clonedResponse);
        response.setAttribute("ID", "_forged_ID");
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
    }

    public void testSignatureWrappingAttackTwo() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signResponse(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 2 - Mangle the contents of the response to be
           <ForgedResponse>
               <LegitimateResponse></LegitimateResponse>
               <LegitimateResponseSignature></LegitimateResponseSignature>
               <ForgedAssertion></ForgedAssertion>
           </ForgedResponse>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element clonedResponse = (Element) response.cloneNode(true);
        final Element clonedSignature = (Element) clonedResponse.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        clonedResponse.removeChild(clonedSignature);
        final Element legitimateSignature = (Element) response.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        response.insertBefore(clonedResponse, legitimateSignature);
        response.setAttribute("ID", "_forged_ID");
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
    }

    /*
     * Most commonly successful XSW attack
     */
    public void testSignatureWrappingAttackThree() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 3 - Mangle the contents of the response to be
           <Response>
               <ForgedAssertion></ForgedAssertion>
               <LegitimateAssertion>
                   <LegitimateAssertionSignature></LegitimateAssertionSignature>
               </LegitimateAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element assertion = (Element) legitimateDocument.
            getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element clonedSignature = (Element) forgedAssertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        forgedAssertion.removeChild(clonedSignature);
        response.insertBefore(forgedAssertion, assertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Expecting only 1 assertion, but response contains multiple"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));

    }


    public void testSignatureWrappingAttackFour() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 4 - Mangle the contents of the response to be
           <Response>
               <ForgedAssertion>
                   <LegitimateAssertion>
                       <LegitimateAssertionSignature></LegitimateAssertionSignature>
                   </LegitimateAssertion>
               </ForgedAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element assertion = (Element) legitimateDocument.getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element clonedSignature = (Element) forgedAssertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        forgedAssertion.removeChild(clonedSignature);
        response.appendChild(forgedAssertion);
        forgedAssertion.appendChild(assertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
    }

    public void testSignatureWrappingAttackFive() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 5 - Mangle the contents of the response to be
           <Response>
               <ForgedAssertion>
                   <LegitimateAssertionSignature></LegitimateAssertionSignature>
               </ForgedAssertion>
               <LegitimateAssertion></LegitimateAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element assertion = (Element) legitimateDocument.getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        final Element signature = (Element) assertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        assertion.removeChild(signature);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element issuer = (Element) forgedAssertion.getElementsByTagNameNS(SAML20_NS, "Issuer").item(0);
        forgedAssertion.insertBefore(signature, issuer.getNextSibling());
        response.insertBefore(forgedAssertion, assertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Expecting only 1 assertion, but response contains multiple"));
    }

    public void testSignatureWrappingAttackSix() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 6 - Mangle the contents of the response to be
           <Response>
               <ForgedAssertion>
                   <LegitimateAssertionSignature>
                       <LegitimateAssertion></LegitimateAssertion>
                   </LegitimateAssertionSignature>
               </ForgedAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element assertion = (Element) legitimateDocument.getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element signature = (Element) assertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        final Element forgedSignature = (Element) forgedAssertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        forgedAssertion.removeChild(forgedSignature);
        assertion.removeChild(signature);
        final Element issuer = (Element) forgedAssertion.getElementsByTagNameNS(SAML20_NS, "Issuer").item(0);
        forgedAssertion.insertBefore(signature, issuer.getNextSibling());
        signature.appendChild(assertion);
        response.appendChild(forgedAssertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
    }

    public void testSignatureWrappingAttackSeven() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 7 - Mangle the contents of the response to be
           <Response>
               <Extensions>
                   <ForgedAssertion><?ForgedAssertion>
               <LegitimateAssertion>
                   <LegitimateAssertionSignature></LegitimateAssertionSignature>
               </LegitimateAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element extensions = legitimateDocument.createElement("Extensions");
        final Element assertion = (Element) legitimateDocument.getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        response.insertBefore(extensions, assertion);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element forgedSignature = (Element) forgedAssertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        forgedAssertion.removeChild(forgedSignature);
        extensions.appendChild(forgedAssertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
    }

    public void testSignatureWrappingAttackEight() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponseAsString(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues(UID_OID);
        assertThat(uid, contains("daredevil"));
        /*
        Permutation 8 - Mangle the contents of the response to be
           <Response>
               <ForgedAssertion>
                   <LegitimateAssertionSignature>
                       <Object>
                           <LegitimateAssertion></LegitimateAssertion>
                       </Object>
                   </LegitimateAssertionSignature>
               </ForgedAssertion>
           </Response>
        */
        final Element response = (Element) legitimateDocument.getElementsByTagNameNS(SAML20P_NS, "Response").item(0);
        final Element assertion = (Element) legitimateDocument.getElementsByTagNameNS(SAML20_NS, "Assertion").item(0);
        final Element forgedAssertion = (Element) assertion.cloneNode(true);
        forgedAssertion.setAttribute("ID", "_forged_assertion_id");
        final Element signature = (Element) assertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        final Element forgedSignature = (Element) forgedAssertion.
            getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "Signature").item(0);
        forgedAssertion.removeChild(forgedSignature);
        assertion.removeChild(signature);
        final Element issuer = (Element) forgedAssertion.getElementsByTagNameNS(SAML20_NS, "Issuer").item(0);
        forgedAssertion.insertBefore(signature, issuer.getNextSibling());
        Element object = legitimateDocument.createElementNS("http://www.w3.org/2000/09/xmldsig#", "Object");
        object.appendChild(assertion);
        signature.appendChild(object);
        response.appendChild(forgedAssertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        assertThat(exception.getCause().getMessage(), containsString("Reference URI did not point to parent ID"));
        assertThat(exception.getCause(), instanceOf(SignatureException.class));
    }

    public void testXXE() throws Exception {
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<!DOCTYPE foo [<!ELEMENT foo ANY > <!ENTITY xxe SYSTEM \"file:///etc/passwd\" >]>" +
            "<foo>&xxe;</foo>";
        final SamlToken token = token(xml);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
        assertThat(exception.getCause().getMessage(), containsString("DOCTYPE"));
    }

    public void testBillionLaughsAttack() throws Exception {
        // There is no need to go up to N iterations
        String xml = "<!DOCTYPE lolz [\n" +
            " <!ENTITY lol \"lol\">\n" +
            " <!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">\n" +
            "]>\n" +
            "<attack>&lol1;</attack>";
        final SamlToken token = token(xml);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
        assertThat(exception.getCause().getMessage(), containsString("DOCTYPE"));
    }

    public void testIgnoredCommentsInForgedResponses() throws Exception {
        final String legitimateNameId = "useradmin@example.com";
        final String forgedNameId = "user<!-- this is a comment -->admin@example.com";
        final String signedXml = signResponse(getSimpleResponseAsString(clock.instant(), legitimateNameId, randomId()));
        final String forgedXml = signedXml.replace(legitimateNameId, forgedNameId);
        final SamlToken forgedToken = token(forgedXml);
        final SamlAttributes attributes = authenticator.authenticate(forgedToken);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(legitimateNameId));
    }

    public void testIgnoredCommentsInLegitimateResponses() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, there is no DOM XMLSignature Factory so we can't sign XML documents", inFipsJvm());
        assumeFalse("Can't run in Azul Zulu JVM", System.getProperty("java.vendor", "").contains("Azul"));

        final String nameId = "user<!-- this is a comment -->admin@example.com";
        final String sanitizedNameId = "useradmin@example.com";
        String xml = getSimpleResponseAsString(clock.instant(), sanitizedNameId, randomId());
        // Need to do this as #getSimpleResponseAsString will escape `<!--` and `-->`
        xml = xml.replace(sanitizedNameId, nameId);
        final Document doc = parseDocument(xml);
        /**
         * If we attempt to parse this to a Response object so that we can sign with {@link #signResponse(String)},
         * {@link SamlRequestHandler#buildXmlObject(Element, Class)} will throw an exception
         * because opensaml's Unmarshaller will fail to unmarshall it because of the comment.
         * So we sign manually with {@link #signElement(Element, String)} which can't be used in FIPS 140 and Azul
         */
        signElement(doc.getDocumentElement(), EXCLUSIVE);
        final SamlToken token = token(SamlUtils.toString(doc.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(sanitizedNameId));
    }

    public void testIgnoredCommentsInResponseUsingCanonicalizationWithComments() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, there is no DOM XMLSignature Factory so we can't sign XML documents", inFipsJvm());
        assumeFalse("Can't run in Azul Zulu JVM",System.getProperty("java.vendor", "").contains("Azul"));

        final String nameId = "user<!-- this is a comment -->admin@example.com";
        final String sanitizedNameId = "useradmin@example.com";
        String xml = getSimpleResponseAsString(clock.instant(), sanitizedNameId, randomId());
        // Need to do this as #getSimpleResponseAsString will escape `<!--` and `-->`
        xml = xml.replace(sanitizedNameId, nameId);
        final Document doc = parseDocument(xml);
        /**
         * If we attempt to parse this to a Response object so that we can sign with {@link #signResponse(String)},
         * {@link SamlRequestHandler#buildXmlObject(Element, Class)} will throw an exception
         * because opensaml's Unmarshaller will fail to unmarshall it because of the comment.
         * So we sign manually with {@link #signElement(Element, String)} which can't be used in FIPS 140 and Azul
         */
        signElement(doc.getDocumentElement(), EXCLUSIVE_WITH_COMMENTS);
        final SamlToken token = token(SamlUtils.toString(doc.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(sanitizedNameId));
    }

    public void testFailureWhenIdPCredentialsAreEmpty() throws Exception {
        authenticator = buildAuthenticator(() -> emptyList(), emptyList());
        final String xml = getSimpleResponseAsString(clock.instant());
        final SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getCause(), nullValue());
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        //Restore the authenticator with credentials for the rest of the test cases
        authenticator = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair), emptyList());
    }

    public void testFailureWhenIdPCredentialsAreNull() throws Exception {
        authenticator = buildAuthenticator(() -> singletonList(null), emptyList());
        final String xml = getSimpleResponseAsString(clock.instant());
        final SamlToken token = token(signResponse(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getCause(), nullValue());
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        //Restore the authenticator with credentials for the rest of the test cases
        authenticator = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair), emptyList());
    }

    private interface CryptoTransform {
        String transform(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception;
    }

    private String signResponse(Response response) throws Exception {
        signSignableObject(response, EXCLUSIVE, SamlAuthenticatorTests.idpSigningCertificatePair);
        return SamlUtils.getXmlContent(response, false);
    }

    private String signResponse(String xml) throws Exception {
        return signResponse(xml, EXCLUSIVE, SamlAuthenticatorTests.idpSigningCertificatePair);
    }

    private String signResponse(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        return signResponse(xml, EXCLUSIVE, keyPair);
    }

    private String signResponse(String xml, String c14nMethod, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        return signResponseString(xml, c14nMethod, keyPair, true);
    }

    private String signAssertions(String xml) throws Exception {
        return signResponseString(xml, EXCLUSIVE, SamlAuthenticatorTests.idpSigningCertificatePair, false);
    }

    private String signAssertions(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        return signResponseString(xml, EXCLUSIVE, keyPair, false);
    }

    private String signResponseString(String xml, String c14nMethod, Tuple<X509Certificate, PrivateKey> keyPair, boolean onlyResponse)
        throws Exception {
        final Response response = toResponse(xml);
        if (onlyResponse) {
            signSignableObject(response, c14nMethod, keyPair);
        } else {
            signSignableObject(response.getAssertions().get(0), c14nMethod, keyPair);
        }
        return SamlUtils.getXmlContent(response, false);
    }

    private Response encryptAssertions(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final Response response = toResponse(xml);
        final Encrypter samlEncrypter = getEncrypter(keyPair);
        EncryptedAssertion encryptedAssertion = samlEncrypter.encrypt(response.getAssertions().get(0));
        response.getAssertions().clear();
        response.getEncryptedAssertions().add(encryptedAssertion);
        return response;
    }

    private Response encryptAttributes(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final Response response = toResponse(xml);
        final Encrypter samlEncrypter = getEncrypter(keyPair);
        final AttributeStatement attributeStatement = response.getAssertions().get(0).getAttributeStatements().get(0);
        for (Attribute plaintextAttribute: attributeStatement.getAttributes()) {
            attributeStatement.getEncryptedAttributes().add(samlEncrypter.encrypt(plaintextAttribute));
        }
        attributeStatement.getAttributes().clear();
        return response;
    }

    private Encrypter getEncrypter(Tuple<X509Certificate, PrivateKey> keyPair) throws Exception{
        final int keyLength = randomFrom(supportedAesKeyLengths);
        final KeyGenerator aesGenerator = KeyGenerator.getInstance("AES");
        aesGenerator.init(keyLength);
        final SecretKey aesKey = aesGenerator.generateKey();
        final Credential dataEncryptionCredential = new BasicCredential(aesKey);
        DataEncryptionParameters encryptionParameters = new DataEncryptionParameters();
        encryptionParameters.setAlgorithm(EncryptionConstants.XMLENC_NS + "aes" + keyLength + "-cbc");
        encryptionParameters.setEncryptionCredential(dataEncryptionCredential);

        final Credential keyEncryptionCredential = new BasicCredential(keyPair.v1().getPublicKey(), keyPair.v2());
        KeyEncryptionParameters keyEncryptionParameters = new KeyEncryptionParameters();
        keyEncryptionParameters.setEncryptionCredential(keyEncryptionCredential);
        keyEncryptionParameters.setAlgorithm(randomFrom(EncryptionConstants.ALGO_ID_KEYTRANSPORT_RSAOAEP,
            EncryptionConstants.ALGO_ID_KEYTRANSPORT_RSA15));

        final Encrypter samlEncrypter = new Encrypter(encryptionParameters, keyEncryptionParameters);
        samlEncrypter.setKeyPlacement(Encrypter.KeyPlacement.INLINE);
        return samlEncrypter;
    }

    private Response toResponse(String xml) throws SAXException, IOException, ParserConfigurationException {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        final Document doc = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        return authenticator.buildXmlObject(doc.getDocumentElement(), Response.class);
    }

    private String getStatusFailedResponse() {
        final Instant now = clock.instant();
        final Response response = SamlUtils.buildObject(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setDestination(SP_ACS_URL);
        response.setID(randomId());
        response.setInResponseTo(requestId);
        response.setIssueInstant(new DateTime(now.toEpochMilli()));
        final Issuer responseIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        responseIssuer.setValue(IDP_ENTITY_ID);
        response.setIssuer(responseIssuer);
        final Status status = SamlUtils.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        final StatusCode statusCode = SamlUtils.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(StatusCode.REQUESTER);
        status.setStatusCode(statusCode);
        response.setStatus(status);

        return SamlUtils.getXmlContent(response, false);
    }

    private String getSimpleResponseAsString(Instant now) {
        return getSimpleResponseAsString(now, randomAlphaOfLengthBetween(12, 18), randomId());
    }

    private String getSimpleResponseAsString(Instant now, String nameId, String sessionindex) {
        final Response response = getSimpleResponse(now, nameId, sessionindex);
        return SamlUtils.getXmlContent(response, false);
    }

    private Response getSimpleResponse(Instant now) {
        return getSimpleResponse(now, randomAlphaOfLengthBetween(12, 18), randomId());
    }

    private Response getSimpleResponse(Instant now, String nameId, String sessionindex) {
        Instant subjectConfirmationValidUntil = now.plusSeconds(120);
        Instant sessionValidUntil = now.plusSeconds(60);
        return getSimpleResponse(now, nameId, sessionindex, subjectConfirmationValidUntil, sessionValidUntil);
    }

    private Response getSimpleResponse(Instant now, String nameId, String sessionindex, Instant subjectConfirmationValidUntil,
                                       Instant sessionValidUntil) {
        final Response response = SamlUtils.buildObject(Response.class, Response.DEFAULT_ELEMENT_NAME);
        response.setDestination(SP_ACS_URL);
        response.setID(randomId());
        response.setInResponseTo(requestId);
        response.setIssueInstant(new DateTime(now.toEpochMilli()));
        final Issuer responseIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        responseIssuer.setValue(IDP_ENTITY_ID);
        response.setIssuer(responseIssuer);
        final Status status = SamlUtils.buildObject(Status.class, Status.DEFAULT_ELEMENT_NAME);
        final StatusCode statusCode = SamlUtils.buildObject(StatusCode.class, StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(StatusCode.SUCCESS);
        status.setStatusCode(statusCode);
        response.setStatus(status);
        final Assertion assertion = SamlUtils.buildObject(Assertion.class, Assertion.DEFAULT_ELEMENT_NAME);
        assertion.setID(sessionindex);
        assertion.setIssueInstant(new DateTime(now.toEpochMilli()));
        final Issuer assertionIssuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        assertionIssuer.setValue(IDP_ENTITY_ID);
        assertion.setIssuer(assertionIssuer);
        AudienceRestriction audienceRestriction = SamlUtils.buildObject(AudienceRestriction.class,
            AudienceRestriction.DEFAULT_ELEMENT_NAME);
        Audience audience = SamlUtils.buildObject(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
        audience.setAudienceURI(SP_ENTITY_ID);
        audienceRestriction.getAudiences().add(audience);
        Conditions conditions = SamlUtils.buildObject(Conditions.class, Conditions.DEFAULT_ELEMENT_NAME);
        conditions.getAudienceRestrictions().add(audienceRestriction);
        assertion.setConditions(conditions);
        final Subject subject = SamlUtils.buildObject(Subject.class, Subject.DEFAULT_ELEMENT_NAME);
        final NameID nameIDElement = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameIDElement.setFormat(TRANSIENT);
        nameIDElement.setNameQualifier(IDP_ENTITY_ID);
        nameIDElement.setSPNameQualifier(SP_ENTITY_ID);
        nameIDElement.setValue(nameId);
        final SubjectConfirmation subjectConfirmation = SamlUtils.buildObject(SubjectConfirmation.class,
            SubjectConfirmation.DEFAULT_ELEMENT_NAME);
        final SubjectConfirmationData subjectConfirmationData = SamlUtils.buildObject(SubjectConfirmationData.class,
            SubjectConfirmationData.DEFAULT_ELEMENT_NAME);
        subjectConfirmationData.setNotOnOrAfter(new DateTime(subjectConfirmationValidUntil.toEpochMilli()));
        subjectConfirmationData.setRecipient(SP_ACS_URL);
        subjectConfirmationData.setInResponseTo(requestId);
        subjectConfirmation.setSubjectConfirmationData(subjectConfirmationData);
        subjectConfirmation.setMethod(METHOD_BEARER);
        subject.setNameID(nameIDElement);
        subject.getSubjectConfirmations().add(subjectConfirmation);
        assertion.setSubject(subject);
        final AuthnContextClassRef authnContextClassRef = SamlUtils.buildObject(AuthnContextClassRef.class,
            AuthnContextClassRef.DEFAULT_ELEMENT_NAME);
        authnContextClassRef.setAuthnContextClassRef(PASSWORD_AUTHN_CTX);
        final AuthnContext authnContext = SamlUtils.buildObject(AuthnContext.class, AuthnContext.DEFAULT_ELEMENT_NAME);
        authnContext.setAuthnContextClassRef(authnContextClassRef);
        final AuthnStatement authnStatement = new AuthnStatementBuilder().buildObject();
        authnStatement.setAuthnContext(authnContext);
        authnStatement.setAuthnInstant(new DateTime(now.toEpochMilli()));
        authnStatement.setSessionIndex(sessionindex);
        authnStatement.setSessionNotOnOrAfter(new DateTime(sessionValidUntil.toEpochMilli()));
        assertion.getAuthnStatements().add(authnStatement);
        final AttributeStatement attributeStatement = SamlUtils.buildObject(AttributeStatement.class,
            AttributeStatement.DEFAULT_ELEMENT_NAME);
        final Attribute attribute1 = SamlUtils.buildObject(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
        attribute1.setNameFormat("urn:oasis:names:tc:SAML:2.0:attrname-format:uri");
        attribute1.setName(UID_OID);
        XSStringBuilder stringBuilder = new XSStringBuilder();
        XSString stringValue1 = stringBuilder.buildObject(AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
        stringValue1.setValue("daredevil");
        attribute1.getAttributeValues().add(stringValue1);
        final Attribute attribute2 = SamlUtils.buildObject(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
        attribute2.setNameFormat("urn:oasis:names:tc:SAML:2.0:attrname-format:uri");
        attribute2.setName("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        XSString stringValue2_1 = stringBuilder.buildObject(AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
        stringValue2_1.setValue("defenders");
        XSString stringValue2_2 = stringBuilder.buildObject(AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
        stringValue2_2.setValue("netflix");
        attribute2.getAttributeValues().add(stringValue2_1);
        attribute2.getAttributeValues().add(stringValue2_2);
        attributeStatement.getAttributes().add(attribute1);
        attributeStatement.getAttributes().add(attribute2);
        assertion.getAttributeStatements().add(attributeStatement);
        response.getAssertions().add(assertion);
        return response;
    }

    private String getSimpleResponseFromXmlTemplate(Instant now, String nameId, String sessionindex) {
        Instant validUntil = now.plusSeconds(30);
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)'"
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(sessionindex)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID  Format='%(TRANSIENT)'"
            + "        NameQualifier='%(IDP_ENTITY_ID)'"
            + "        SPNameQualifier='%(SP_ENTITY_ID)'>%(nameId)</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "        <assert:SubjectConfirmationData NotOnOrAfter='%(validUntil)' Recipient='%(SP_ACS_URL)' InResponseTo='%(requestId)'/>"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:AuthnStatement AuthnInstant='%(now)' SessionNotOnOrAfter='%(validUntil)' SessionIndex='%(sessionindex)'>"
            + "      <assert:AuthnContext>"
            + "        <assert:AuthnContextClassRef>%(PASSWORD_AUTHN_CTX)</assert:AuthnContextClassRef>"
            + "      </assert:AuthnContext>"
            + "    </assert:AuthnStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "         NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "         Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "         NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri' Name='urn:oid:1.3.6.1.4.1.5923.1.5.1.1'>"
            + "      <assert:AttributeValue xsi:type='xs:string'>defenders</assert:AttributeValue>"
            + "      <assert:AttributeValue xsi:type='xs:string'>netflix</assert:AttributeValue>"
            + "    </assert:Attribute></assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("nameId", nameId);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        return NamedFormatter.format(xml, replacements);
    }

    private SamlToken token(String content) {
        return token(content.getBytes(StandardCharsets.UTF_8));
    }

    private SamlToken token(byte[] content) {
        return new SamlToken(content, singletonList(requestId), null);
    }

}
