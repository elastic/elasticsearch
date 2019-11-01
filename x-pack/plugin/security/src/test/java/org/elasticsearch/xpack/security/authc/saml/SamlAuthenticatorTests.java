/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.xml.security.Init;
import org.apache.xml.security.encryption.EncryptedData;
import org.apache.xml.security.encryption.EncryptedKey;
import org.apache.xml.security.encryption.EncryptionMethod;
import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.exceptions.XMLSecurityException;
import org.apache.xml.security.keys.content.X509Data;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.encryption.support.DecryptionException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.xml.crypto.dsig.CanonicalizationMethod;
import javax.xml.crypto.dsig.DigestMethod;
import javax.xml.crypto.dsig.Reference;
import javax.xml.crypto.dsig.SignatureMethod;
import javax.xml.crypto.dsig.SignedInfo;
import javax.xml.crypto.dsig.Transform;
import javax.xml.crypto.dsig.XMLSignature;
import javax.xml.crypto.dsig.XMLSignatureFactory;
import javax.xml.crypto.dsig.dom.DOMSignContext;
import javax.xml.crypto.dsig.keyinfo.KeyInfo;
import javax.xml.crypto.dsig.keyinfo.KeyInfoFactory;
import javax.xml.crypto.dsig.spec.C14NMethodParameterSpec;
import javax.xml.crypto.dsig.spec.TransformParameterSpec;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE;
import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE_WITH_COMMENTS;
import static javax.xml.crypto.dsig.Transform.ENVELOPED;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
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
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_ATTRIB_NAME;
import static org.opensaml.saml.saml2.core.SubjectConfirmation.METHOD_BEARER;

public class SamlAuthenticatorTests extends SamlTestCase {

    private static final String SP_ENTITY_ID = "https://sp.saml.elastic.test/";
    private static final String IDP_ENTITY_ID = "https://idp.saml.elastic.test/";
    private static final String SP_ACS_URL = SP_ENTITY_ID + "sso/post";

    private static Tuple<X509Certificate, PrivateKey> idpSigningCertificatePair;
    private static Tuple<X509Certificate, PrivateKey> spSigningCertificatePair;
    private static List<Tuple<X509Certificate, PrivateKey>> spEncryptionCertificatePairs;

    private static List<Integer> supportedAesKeyLengths;
    private static List<String> supportedAesTransformations;

    private ClockMock clock;
    private SamlAuthenticator authenticator;
    private String requestId;
    private TimeValue maxSkew;

    @BeforeClass
    public static void init() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, there is no DOM XMLSignature Factory so we can't sign XML documents", inFipsJvm());
        // TODO: Refactor the signing to use org.opensaml.xmlsec.signature.support.Signer so that we can run the tests
        SamlUtils.initialize(LogManager.getLogger(SamlAuthenticatorTests.class));
        // Initialise Apache XML security so that the signDoc methods work correctly.
        Init.init();
    }

    @BeforeClass
    public static void calculateAesLength() throws NoSuchAlgorithmException {
        supportedAesKeyLengths = new ArrayList<>();
        supportedAesTransformations = new ArrayList<>();
        supportedAesKeyLengths.add(128);
        supportedAesTransformations.add(XMLCipher.AES_128);
        supportedAesTransformations.add(XMLCipher.AES_128_GCM);
        if (Cipher.getMaxAllowedKeyLength("AES") > 128) {
            supportedAesKeyLengths.add(192);
            supportedAesKeyLengths.add(256);
            supportedAesTransformations.add(XMLCipher.AES_192);
            supportedAesTransformations.add(XMLCipher.AES_192_GCM);
            supportedAesTransformations.add(XMLCipher.AES_256);
            supportedAesTransformations.add(XMLCipher.AES_256_GCM);
        }
    }

    /**
     * Generating X.509 credentials can be CPU intensive and slow, so we only want to do it once per class.
     */
    @BeforeClass
    public static void initCredentials() throws Exception {
        idpSigningCertificatePair = readRandomKeyPair(randomSigningAlgorithm());
        spSigningCertificatePair = readRandomKeyPair(randomSigningAlgorithm());
        spEncryptionCertificatePairs = Arrays.asList(readKeyPair("RSA_2048"), readKeyPair("RSA_4096"));
    }

    private static String randomSigningAlgorithm() {
        return randomFrom("RSA", "DSA", "EC");
    }

    @AfterClass
    public static void cleanup() {
        idpSigningCertificatePair = null;
        spSigningCertificatePair = null;
        spEncryptionCertificatePairs = null;
        supportedAesKeyLengths = null;
        supportedAesTransformations = null;
    }

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
        Instant now = clock.instant();
        final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<saml2p:Response"
            + "    Destination=\"%(SP_ACS_URL)\""
            + "    ID=\"%(randomId)\""
            + "    InResponseTo=\"%(requestId)\""
            + "    IssueInstant=\"%(now)\""
            + "    Version=\"2.0\""
            + "    xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "  <saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">"
            + "    %(IDP_ENTITY_ID)"
            + "  </saml2:Issuer>"
            + "  <saml2p:Status>"
            + "     <saml2p:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>"
            + "  </saml2p:Status>"
            + "</saml2p:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", now);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);

        SamlToken token = token(NamedFormatter.format(xml, replacements));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("No assertions found in SAML response"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testSuccessfullyParseContentWithASingleValidAssertion() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String nameId = randomAlphaOfLengthBetween(12, 24);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response "
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)'"
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#'>"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status>"
            + "    <proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/>"
            + "  </proto:Status>"
            + "  <assert:Assertion ID='%(sessionindex)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID Format='%(TRANSIENT)'>%(nameId)</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
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
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(1));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));
        assertThat(uid, iterableWithSize(1));
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(nameId));
    }

    public void testSuccessfullyParseContentWithMultipleValidAttributes() throws Exception {
        final String nameId = randomAlphaOfLengthBetween(4, 8) + "-" + randomAlphaOfLengthBetween(8, 12);
        final String session = randomId();

        final String xml = getSimpleResponse(clock.instant(), nameId, session);
        SamlToken token = token(signDoc(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));

        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(NameID.TRANSIENT));
        assertThat(attributes.name().value, equalTo(nameId));
        assertThat(attributes.name().idpNameQualifier, equalTo(IDP_ENTITY_ID));
        assertThat(attributes.name().spNameQualifier, equalTo(SP_ENTITY_ID));

        assertThat(attributes.session(), equalTo(session));
    }

    public void testSuccessfullyParseContentFromEncryptedAssertion() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponse(now);

        final String encrypted = encryptAssertions(xml, randomFrom(spEncryptionCertificatePairs));
        assertThat(encrypted, not(equalTo(xml)));

        final String signed = signDoc(encrypted);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testSuccessfullyParseContentFromEncryptedAndSignedAssertion() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponse(now);

        final String signed = processAssertions(parseDocument(xml), element -> {
            // For the signature to validate, it needs to be made against a fragment assertion, so we:
            // - convert the assertion to an xml-string
            // - parse it into a new doc
            // - sign it there
            // - then replace it in the original doc
            // This is the most reliable way to get a valid signature
            final String str = SamlUtils.toString(element);
            final Element clone = parseDocument(str).getDocumentElement();
            signElement(clone, idpSigningCertificatePair);
            element.getOwnerDocument().adoptNode(clone);
            element.getParentNode().replaceChild(clone, element);
        });

        assertThat(signed, not(equalTo(xml)));
        final String encrypted = encryptAssertions(signed, randomFrom(spEncryptionCertificatePairs));
        assertThat(encrypted, not(equalTo(signed)));

        final SamlToken token = token(encrypted);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testSuccessfullyParseContentFromEncryptedAttribute() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signDoc : this::signAssertions;
        final Instant now = clock.instant();
        final String xml = getSimpleResponse(now);

        final String encrypted = encryptAttributes(xml, randomFrom(spEncryptionCertificatePairs));
        assertThat(encrypted, not(equalTo(xml)));

        final String signed = signer.transform(encrypted, idpSigningCertificatePair);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));

        final List<String> groups = attributes.getAttributeValues("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        assertThat(groups, containsInAnyOrder("defenders", "netflix"));
    }

    public void testFailWhenAssertionsCannotBeDecrypted() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponse(now);

        // Encrypting with different cert instead of sp cert will mean that the SP cannot decrypt
        final String encrypted = encryptAssertions(xml, readKeyPair("RSA_4096_updated"));
        assertThat(encrypted, not(equalTo(xml)));

        final String signed = signDoc(encrypted);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Failed to decrypt"));
        assertThat(exception.getCause(), instanceOf(DecryptionException.class));
    }

    public void testNoAttributesReturnedWhenTheyCannotBeDecrypted() throws Exception {
        final Instant now = clock.instant();
        final String xml = getSimpleResponse(now);

        // Encrypting with different cert instead of sp cert will mean that the SP cannot decrypt
        final String encrypted = encryptAttributes(xml, readKeyPair("RSA_4096_updated"));
        assertThat(encrypted, not(equalTo(xml)));

        final String signed = signDoc(encrypted);
        assertThat(signed, not(equalTo(encrypted)));

        final SamlToken token = token(signed);
        // Because an assertion can theoretically contains encrypted and unencrypted attributes
        // we don't treat a decryption as a hard failure.
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.attributes(), iterableWithSize(0));
    }

    public void testIncorrectResponseIssuerIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response "
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
            + "    IssueInstant='%(now)' Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)xxx</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(sessionindex)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Issuer"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectAssertionIssuerIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)'"
            + "    IssueInstant='%(now)' Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(sessionindex)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)_</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Issuer"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectDestinationIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)/fake'"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        final String xmlWithReplacements = NamedFormatter.format(xml, replacements);

        SamlToken token = randomBoolean()
            ? token(signDoc(xmlWithReplacements))
            : token(signAssertions(xmlWithReplacements, idpSigningCertificatePair));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("destination"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testMissingDestinationIsNotRejectedForNotSignedResponse() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signAssertions(NamedFormatter.format(xml, replacements), idpSigningCertificatePair));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(1));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
        assertThat(uid, contains("daredevil"));
        assertThat(uid, iterableWithSize(1));
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
    }

    public void testIncorrectRequestIdIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String incorrectId = "_012345";
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(incorrectId)' "
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("incorrectId", incorrectId);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("in-response-to"));
        assertThat(exception.getMessage(), containsString(requestId));
        assertThat(exception.getMessage(), containsString(incorrectId));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectRecipientIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "     Destination='%(SP_ACS_URL)'"
            + "     ID='%(randomId)'"
            + "     InResponseTo='%(requestId)'"
            + "     IssueInstant='%(now)'"
            + "     Version='2.0'"
            + "     xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "     xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "     xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "     xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "     xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(sessionindex)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "      <assert:SubjectConfirmationData"
            + "          NotOnOrAfter='%(validUntil)'"
            + "          Recipient='%(SP_ACS_URL)/fake'"
            + "          InResponseTo='%(requestId)' />"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:AuthnStatement AuthnInstant='%(now)' SessionNotOnOrAfter='%(validUntil)' SessionIndex='%(sessionindex)'>"
            + "      <assert:AuthnContext>"
            + "        <assert:AuthnContextClassRef>%(PASSWORD_AUTHN_CTX)</assert:AuthnContextClassRef>"
            + "      </assert:AuthnContext>"
            + "    </assert:AuthnStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion SubjectConfirmationData Recipient"));
        assertThat(exception.getMessage(), containsString(SP_ACS_URL + "/fake"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectIsRejected() throws Exception {
        Instant now = clock.instant();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "     Destination='%(SP_ACS_URL)'"
            + "     ID='%(randomId)'"
            + "     InResponseTo='%(requestId)' "
            + "     IssueInstant='%(now)'"
            + "     Version='2.0'"
            + "     xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "     xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "     xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "     xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "     xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(randomId2)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute"
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("randomId2", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("has no Subject"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutAuthnStatementIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(randomId2)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "        <assert:SubjectConfirmationData NotOnOrAfter='%(validUntil)' Recipient='%(SP_ACS_URL)' InResponseTo='%(requestId)'/>"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("randomId2", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("Authn Statements while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testExpiredAuthnStatementSessionIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(120);
        Instant sessionValidUntil = now.plusSeconds(60);
        final String nameId = randomAlphaOfLengthBetween(12, 24);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
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
            + "      <assert:NameID Format='%(TRANSIENT)'>%(nameId)</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "        <assert:SubjectConfirmationData NotOnOrAfter='%(validUntil)' Recipient='%(SP_ACS_URL)' InResponseTo='%(requestId)'/>"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:AuthnStatement AuthnInstant='%(now)' SessionNotOnOrAfter='%(sessionValidUntil)' SessionIndex='%(sessionindex)'>"
            + "      <assert:AuthnContext>"
            + "        <assert:AuthnContextClassRef>%(PASSWORD_AUTHN_CTX)</assert:AuthnContextClassRef>"
            + "      </assert:AuthnContext>"
            + "    </assert:AuthnStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
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
        replacements.put("sessionValidUntil", sessionValidUntil);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        // check that the content is valid "now"
        final SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
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
        Instant validUntil = now.plusSeconds(30);
        final String nameId = randomAlphaOfLengthBetween(12, 24);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
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
            + "      <assert:NameID Format='%(TRANSIENT)'>%(nameId)</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
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
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlAuthenticator authenticatorWithReqAuthnCtx = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair),
            Arrays.asList(X509_AUTHN_CTX, KERBEROS_AUTHN_CTX));
        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticatorWithReqAuthnCtx.authenticate(token));
        assertThat(exception.getMessage(), containsString("Rejecting SAML assertion as the AuthnContextClassRef"));
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectConfirmationIsRejected() throws Exception {
        Instant now = clock.instant();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(randomId2)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "    </assert:Subject>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", now);
        replacements.put("randomId", randomId());
        replacements.put("randomId2", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion subject contains [0] bearer SubjectConfirmation"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssertionWithoutSubjectConfirmationDataIsRejected() throws Exception {
        Instant now = clock.instant();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Success'/></proto:Status>"
            + "  <assert:Assertion ID='%(randomId2)' IssueInstant='%(now)' Version='2.0'>"
            + "    <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "    <assert:Subject>"
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'/>"
            + "    </assert:Subject>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("randomId", randomId());
        replacements.put("randomId2", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("bearer SubjectConfirmation, while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testAssetionWithoutBearerSubjectConfirmationMethodIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_ATTRIB_NAME)'>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_ATTRIB_NAME", METHOD_ATTRIB_NAME);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("bearer SubjectConfirmation, while exactly one was expected."));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testIncorrectSubjectConfirmationDataInResponseToIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String incorrectId = "_123456";
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
            + "    InResponseTo='%(requestId)' "
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "        <assert:SubjectConfirmationData"
            + "            NotOnOrAfter='%(validUntil)'"
            + "            Recipient='%(SP_ACS_URL)'"
            + "            InResponseTo='%(incorrectId)'/>"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:AuthnStatement AuthnInstant='%(now)' SessionNotOnOrAfter='%(validUntil)' SessionIndex='%(sessionindex)'>"
            + "      <assert:AuthnContext>"
            + "        <assert:AuthnContextClassRef>%(PASSWORD_AUTHN_CTX)</assert:AuthnContextClassRef>"
            + "      </assert:AuthnContext>"
            + "    </assert:AuthnStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("incorrectId", incorrectId);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("SAML Assertion SubjectConfirmationData is in-response-to"));
        assertThat(exception.getMessage(), containsString(requestId));
        assertThat(exception.getMessage(), containsString(incorrectId));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testExpiredSubjectConfirmationDataIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(120);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        // check that the content is valid "now"
        final SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
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
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)'"
            + "    ID='%(randomId)'"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        final SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(1));
    }

    public void testIncorrectSigningKeyIsRejected() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signDoc : this::signAssertions;
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        final String xmlWithReplacements = NamedFormatter.format(xml, replacements);

        // check that the content is valid when signed by the correct key-pair
        assertThat(authenticator.authenticate(token(signer.transform(xmlWithReplacements, idpSigningCertificatePair))), notNullValue());

        // check is rejected when signed by a different key-pair
        final Tuple<X509Certificate, PrivateKey> wrongKey = readKeyPair("RSA_4096_updated");
        final ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticator.authenticate(token(signer.transform(xmlWithReplacements, wrongKey)))
        );
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testSigningKeyIsReloadedForEachRequest() throws Exception {
        final CryptoTransform signer = randomBoolean() ? this::signDoc : this::signAssertions;
        final String xml = getSimpleResponse(Instant.now());

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
        CryptoTransform signer = randomBoolean() ? this::signDoc : this::signAssertions;
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        // check that the original signed content is valid
        final String signed = signer.transform(NamedFormatter.format(xml, replacements), idpSigningCertificatePair);
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
        final CryptoTransform signer = randomBoolean() ? this::signDoc : this::signAssertions;
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(30);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
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
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        final String xmlWithReplacements = NamedFormatter.format(xml, replacements);

        // check that the content is valid when signed by the each of the key-pairs
        for (Tuple<X509Certificate, PrivateKey> key : keys) {
            assertThat(authenticator.authenticate(token(signer.transform(xmlWithReplacements, key))), notNullValue());
        }
    }

    /**
     * This is testing a test, but the real signing tests are useless if our signing is incorrectly implemented
     */
    public void testThatTheTestSignersInteractCorrectlyWithOpenSaml() throws Exception {
        final String xml = getSimpleResponse(clock.instant());
        final Response unsigned = toResponse(xml);
        assertThat(unsigned.isSigned(), equalTo(false));
        assertThat(unsigned.getAssertions().get(0).isSigned(), equalTo(false));

        final Response signedDoc = toResponse(signDoc(xml, idpSigningCertificatePair));
        assertThat(signedDoc.isSigned(), equalTo(true));
        assertThat(signedDoc.getAssertions().get(0).isSigned(), equalTo(false));

        final Response signedAssertions = toResponse(signAssertions(xml, idpSigningCertificatePair));
        assertThat(signedAssertions.isSigned(), equalTo(false));
        assertThat(signedAssertions.getAssertions().get(0).isSigned(), equalTo(true));
    }

    /**
     * This is testing a test, but the real encryption tests are useless if our encryption routines don't do anything
     */
    public void testThatTheTestEncryptionInteractsCorrectlyWithOpenSaml() throws Exception {
        final String xml = getSimpleResponse(clock.instant());

        final Response unencrypted = toResponse(xml);
        // Expect Assertion > AttributeStatement (x2) > Attribute
        assertThat(unencrypted.getAssertions(), iterableWithSize(1));
        assertThat(unencrypted.getEncryptedAssertions(), iterableWithSize(0));
        for (Assertion assertion : unencrypted.getAssertions()) {
            assertThat(assertion.getAttributeStatements(), iterableWithSize(2));
            for (AttributeStatement statement : assertion.getAttributeStatements()) {
                assertThat(statement.getAttributes(), iterableWithSize(1));
                assertThat(statement.getEncryptedAttributes(), iterableWithSize(0));
            }
        }

        final Tuple<X509Certificate, PrivateKey> spEncryptionCertificatePair = randomFrom(spEncryptionCertificatePairs);
        final Response encryptedAssertion = toResponse(encryptAssertions(xml, spEncryptionCertificatePair));
        // Expect EncryptedAssertion
        assertThat(encryptedAssertion.getAssertions(), iterableWithSize(0));
        assertThat(encryptedAssertion.getEncryptedAssertions(), iterableWithSize(1));

        final Response encryptedAttributes = toResponse(encryptAttributes(xml, spEncryptionCertificatePair));
        // Expect Assertion > AttributeStatement (x2) > EncryptedAttribute
        assertThat(encryptedAttributes.getAssertions(), iterableWithSize(1));
        assertThat(encryptedAttributes.getEncryptedAssertions(), iterableWithSize(0));
        for (Assertion assertion : encryptedAttributes.getAssertions()) {
            assertThat(assertion.getAttributeStatements(), iterableWithSize(2));
            for (AttributeStatement statement : assertion.getAttributeStatements()) {
                assertThat(statement.getAttributes(), iterableWithSize(0));
                assertThat(statement.getEncryptedAttributes(), iterableWithSize(1));
            }
        }
    }

    public void testExpiredContentIsRejected() throws Exception {
        Instant now = clock.instant();
        Instant validUntil = now.plusSeconds(120);
        final String sessionindex = randomId();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
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
            + "      <assert:NameID SPNameQualifier='%(SP_ENTITY_ID)' Format='%(TRANSIENT)'>randomopaquestring</assert:NameID>"
            + "      <assert:SubjectConfirmation Method='%(METHOD_BEARER)'>"
            + "        <assert:SubjectConfirmationData NotOnOrAfter='%(validUntil)' Recipient='%(SP_ACS_URL)' InResponseTo='%(requestId)'/>"
            + "      </assert:SubjectConfirmation>"
            + "    </assert:Subject>"
            + "    <assert:Conditions NotBefore='%(now)' NotOnOrAfter='%(validUntil)'></assert:Conditions>"
            + "    <assert:AuthnStatement AuthnInstant='%(now)' SessionNotOnOrAfter='%(validUntil)' SessionIndex='%(sessionindex)'>"
            + "      <assert:AuthnContext>"
            + "        <assert:AuthnContextClassRef>%(PASSWORD_AUTHN_CTX)</assert:AuthnContextClassRef>"
            + "      </assert:AuthnContext>"
            + "    </assert:AuthnStatement>"
            + "    <assert:AttributeStatement>"
            + "      <assert:Attribute "
            + "          NameFormat='urn:oasis:names:tc:SAML:2.0:attrname-format:uri'"
            + "          Name='urn:oid:0.9.2342.19200300.100.1.1'>"
            + "        <assert:AttributeValue xsi:type='xs:string'>daredevil</assert:AttributeValue>"
            + "      </assert:Attribute>"
            + "    </assert:AttributeStatement>"
            + "  </assert:Assertion>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("METHOD_BEARER", METHOD_BEARER);
        replacements.put("now", now);
        replacements.put("PASSWORD_AUTHN_CTX", PASSWORD_AUTHN_CTX);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("sessionindex", sessionindex);
        replacements.put("SP_ACS_URL", SP_ACS_URL);
        replacements.put("SP_ENTITY_ID", SP_ENTITY_ID);
        replacements.put("TRANSIENT", TRANSIENT);
        replacements.put("validUntil", validUntil);

        // check that the content is valid "now"
        final SamlToken token = token(signDoc(NamedFormatter.format(xml, replacements)));
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
        final String xml = getResponseWithAudienceRestrictions(audience);
        final SamlToken token = token(signDoc(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getMessage(), containsString("required audience"));
        assertThat(exception.getMessage(), containsString(audience));
        assertThat(exception.getMessage(), containsString(SP_ENTITY_ID));
        assertThat(exception.getCause(), nullValue());
        assertThat(SamlUtils.isSamlException(exception), is(true));
    }

    public void testContentIsAcceptedIfRestrictedToOurAudience() throws Exception {
        final String xml = getResponseWithAudienceRestrictions(SP_ENTITY_ID);
        final SamlToken token = token(signDoc(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), not(empty()));
    }

    public void testLoggingWhenAudienceCheckFails() throws Exception {
        final String similarAudience = SP_ENTITY_ID.replaceFirst("/$", ":80/");
        final String wrongAudience = "http://" + randomAlphaOfLengthBetween(4, 12) + "." + randomAlphaOfLengthBetween(6, 8) + "/";
        final String xml = getResponseWithAudienceRestrictions(similarAudience, wrongAudience);
        final SamlToken token = token(signDoc(xml));

        final Logger samlLogger = LogManager.getLogger(authenticator.getClass());
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(samlLogger, mockAppender);

            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "similar audience",
                authenticator.getClass().getName(),
                Level.INFO,
                "Audience restriction [" + similarAudience + "] does not match required audience [" + SP_ENTITY_ID +
                    "] (difference starts at character [#" + (SP_ENTITY_ID.length() - 1) + "] [:80/] vs [/])"
            ));
            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "not similar audience",
                authenticator.getClass().getName(),
                Level.INFO,
                "Audience restriction [" + wrongAudience + "] does not match required audience [" + SP_ENTITY_ID + "]"
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
        final SamlToken token = token(signDoc(xml));
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signDoc(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signDoc(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        final String xml = getSimpleResponse(now);
        final Document legitimateDocument = parseDocument(signAssertions(xml, idpSigningCertificatePair));
        // First verify that the correct SAML Response can be consumed
        final SamlToken legitimateToken = token(SamlUtils.toString(legitimateDocument.getDocumentElement()));
        final SamlAttributes attributes = authenticator.authenticate(legitimateToken);
        assertThat(attributes, notNullValue());
        assertThat(attributes.attributes(), iterableWithSize(2));
        final List<String> uid = attributes.getAttributeValues("urn:oid:0.9.2342.19200300.100.1.1");
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
        Element object = legitimateDocument.createElement("Object");
        object.appendChild(assertion);
        signature.appendChild(object);
        response.appendChild(forgedAssertion);
        final SamlToken forgedToken = token(SamlUtils.toString((legitimateDocument.getDocumentElement())));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(forgedToken));
        assertThat(exception.getMessage(), containsString("Failed to parse SAML"));
        assertThat(exception.getCause(), instanceOf(SAXException.class));
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
        final String signedXml = signDoc(getSimpleResponse(clock.instant(), legitimateNameId, randomId()));
        final String forgedXml = signedXml.replace(legitimateNameId, forgedNameId);
        final SamlToken forgedToken = token(forgedXml);
        final SamlAttributes attributes = authenticator.authenticate(forgedToken);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(legitimateNameId));
    }

    public void testIgnoredCommentsInLegitimateResponses() throws Exception {
        final String nameId = "user<!-- this is a comment -->admin@example.com";
        final String sanitizedNameId = "useradmin@example.com";
        final String xml = getSimpleResponse(clock.instant(), nameId, randomId());
        final SamlToken token = token(signDoc(xml));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(sanitizedNameId));
    }

    public void testIgnoredCommentsInResponseUsingCanonicalizationWithComments() throws Exception {
        final String nameId = "user<!-- this is a comment -->admin@example.com";
        final String sanitizedNameId = "useradmin@example.com";
        final String xml = getSimpleResponse(clock.instant(), nameId, randomId());
        final SamlToken token = token(signDoc(xml, EXCLUSIVE_WITH_COMMENTS));
        final SamlAttributes attributes = authenticator.authenticate(token);
        assertThat(attributes.name(), notNullValue());
        assertThat(attributes.name().format, equalTo(TRANSIENT));
        assertThat(attributes.name().value, equalTo(sanitizedNameId));
    }

    public void testFailureWhenIdPCredentialsAreEmpty() throws Exception {
        authenticator = buildAuthenticator(() -> emptyList(), emptyList());
        final String xml = getSimpleResponse(clock.instant());
        final SamlToken token = token(signDoc(xml));
        final ElasticsearchSecurityException exception = expectSamlException(() -> authenticator.authenticate(token));
        assertThat(exception.getCause(), nullValue());
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
        //Restore the authenticator with credentials for the rest of the test cases
        authenticator = buildAuthenticator(() -> buildOpenSamlCredential(idpSigningCertificatePair), emptyList());
    }

    public void testFailureWhenIdPCredentialsAreNull() throws Exception {
        authenticator = buildAuthenticator(() -> singletonList(null), emptyList());
        final String xml = getSimpleResponse(clock.instant());
        final SamlToken token = token(signDoc(xml));
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

    private String signDoc(String xml) throws Exception {
        return signDoc(xml, EXCLUSIVE, SamlAuthenticatorTests.idpSigningCertificatePair);
    }

    private String signDoc(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        return signDoc(xml, EXCLUSIVE, keyPair);
    }

    private String signDoc(String xml, String c14nMethod) throws Exception {
        return signDoc(xml, c14nMethod, SamlAuthenticatorTests.idpSigningCertificatePair);
    }

    private String signDoc(String xml, String c14nMethod, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final Document doc = parseDocument(xml);
        signElement(doc.getDocumentElement(), keyPair, c14nMethod);
        return SamlUtils.toString(doc.getDocumentElement());
    }

    private String signAssertions(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final Document doc = parseDocument(xml);
        return processAssertions(doc, node -> signElement(node, keyPair));
    }

    private String encryptAssertions(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final X509Certificate certificate = keyPair.v1();
        final Document doc = parseDocument(xml);
        // Some Identity Providers (e.g. Shibboleth) include the AES <EncryptedKey> directly within the <EncryptedData> element
        // And some (e.g. Okta) include a <RetrievalMethod> that links to an <EncryptedKey> that is a sibling of the <EncryptedData>
        final boolean withRetrievalMethod = randomBoolean();
        return processAssertions(doc, node -> wrapAndEncrypt(node, "Assertion", certificate, withRetrievalMethod));
    }

    private String encryptAttributes(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final X509Certificate certificate = keyPair.v1();
        final Document doc = parseDocument(xml);
        return processAttributes(doc, node -> wrapAndEncrypt(node, "Attribute", certificate, false));
    }

    private String processAssertions(Document doc, CheckedConsumer<Element, Exception> consumer) throws Exception {
        return processNodes(doc, SAML20_NS, "Assertion", consumer);
    }

    private String processAttributes(Document doc, CheckedConsumer<Element, Exception> consumer) throws Exception {
        return processNodes(doc, SAML20_NS, "Attribute", consumer);
    }

    private String processNodes(Document doc, String namespaceURI, String localName, CheckedConsumer<Element, Exception> consumer)
            throws Exception {
        final NodeList nodes = doc.getElementsByTagNameNS(namespaceURI, localName);
        // Because the consumer changes the nodes (and removes them from the document) we need to clone the list
        List<Node> list = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
            list.add(nodes.item(i));
        }
        for (Node node : list) {
            consumer.accept((Element) node);
        }
        return SamlUtils.toString(doc.getDocumentElement());
    }

    private Document parseDocument(String xml) throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        final DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
        return documentBuilder.parse(new InputSource(new StringReader(xml)));
    }

    private void signElement(Element parent, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        signElement(parent, keyPair, EXCLUSIVE);
    }

    /**
     * Randomly selects digital signature algorithm URI for given private key
     * algorithm ({@link PrivateKey#getAlgorithm()}).
     *
     * @param key
     *            {@link PrivateKey}
     * @return algorithm URI
     */
    private String getSignatureAlgorithmURI(PrivateKey key) {
        String algoUri = null;
        switch (key.getAlgorithm()) {
        case "RSA":
            algoUri = randomFrom("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
                    "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512");
            break;
        case "DSA":
            algoUri = "http://www.w3.org/2009/xmldsig11#dsa-sha256";
            break;
        case "EC":
            algoUri = randomFrom("http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha256",
                    "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha512");
            break;
        default:
            throw new IllegalArgumentException("Unsupported algorithm : " + key.getAlgorithm()
                    + " for signature, allowed values for private key algorithm are [RSA, DSA, EC]");
        }
        return algoUri;
    }

    private void signElement(Element parent, Tuple<X509Certificate, PrivateKey> keyPair, String c14nMethod) throws Exception {
        //We need to explicitly set the Id attribute, "ID" is just our convention
        parent.setIdAttribute("ID", true);
        final String refID = "#" + parent.getAttribute("ID");
        final X509Certificate certificate = keyPair.v1();
        final PrivateKey privateKey = keyPair.v2();
        final XMLSignatureFactory fac = XMLSignatureFactory.getInstance("DOM");
        final DigestMethod digestMethod = fac.newDigestMethod(randomFrom(DigestMethod.SHA256, DigestMethod.SHA512), null);
        final Transform transform = fac.newTransform(ENVELOPED, (TransformParameterSpec) null);
        // We don't "have to" set the reference explicitly since we're using enveloped signatures, but it helps with
        // creating the XSW test cases
        final Reference reference = fac.newReference(refID, digestMethod, singletonList(transform), null, null);
        final SignatureMethod signatureMethod = fac.newSignatureMethod(getSignatureAlgorithmURI(privateKey), null);
        final CanonicalizationMethod canonicalizationMethod = fac.newCanonicalizationMethod(c14nMethod, (C14NMethodParameterSpec) null);

        final SignedInfo signedInfo = fac.newSignedInfo(canonicalizationMethod, signatureMethod, singletonList(reference));

        final KeyInfo keyInfo = getKeyInfo(fac, certificate);

        final DOMSignContext dsc = new DOMSignContext(privateKey, parent);
        dsc.setDefaultNamespacePrefix("ds");
        // According to the schema, the signature needs to be placed after the <Issuer> if there is one in the document
        // If there are more than one <Issuer> we are dealing with a <Response> so we sign the Response and add the
        // Signature after the Response <Issuer>
        NodeList issuersList = parent.getElementsByTagNameNS(SAML20_NS, "Issuer");
        if (issuersList.getLength() > 0) {
            dsc.setNextSibling(issuersList.item(0).getNextSibling());
        }

        final XMLSignature signature = fac.newXMLSignature(signedInfo, keyInfo);
        signature.sign(dsc);
    }

    private static KeyInfo getKeyInfo(XMLSignatureFactory factory, X509Certificate certificate) throws KeyException {
        KeyInfoFactory kif = factory.getKeyInfoFactory();
        javax.xml.crypto.dsig.keyinfo.X509Data data = kif.newX509Data(Collections.singletonList(certificate));
        return kif.newKeyInfo(singletonList(data));
    }

    private void wrapAndEncrypt(Node node, String tagName, X509Certificate certificate, boolean withRetrievalMethod) throws Exception {
        assertThat(node, instanceOf(Element.class));
        final Element element = (Element) node;
        assertThat(element.getLocalName(), equalTo(tagName));

        // Wrap the assertion in an "EncryptedXXX" element and then replace it with the encrypted content
        final Node parent = element.getParentNode();
        final Element encryptedWrapper = parent.getOwnerDocument().createElementNS(element.getNamespaceURI(), "Encrypted" + tagName);
        parent.replaceChild(encryptedWrapper, element);
        encryptedWrapper.appendChild(element);

        // The node, once encrypted needs to be "standalone", so it needs to have all the namespaces defined locally.
        // There might be a more standard way to do this, but this works...
        defineRequiredNamespaces(element);
        encryptElement(element, certificate, withRetrievalMethod);
    }

    private void defineRequiredNamespaces(Element element) {
        defineRequiredNamespaces(element, Collections.emptySet());
    }

    private void defineRequiredNamespaces(Element element, Set<Tuple<String, String>> parentProcessed) {
        Set<Tuple<String, String>> processed = new HashSet<>(parentProcessed);
        final Map<String, String> namespaces = getNamespaces(element);
        for (String prefix : namespaces.keySet()) {
            final String uri = namespaces.get(prefix);
            Tuple<String, String> t = new Tuple<>(prefix, uri);
            if (processed.contains(t) == false) {
                processed.add(t);
                if (Strings.isNullOrEmpty(element.getAttribute("xmlns:" + prefix))) {
                    element.setAttribute("xmlns:" + prefix, uri);
                }
            }
        }
        final NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            final Node child = children.item(i);
            if (child instanceof Element) {
                defineRequiredNamespaces((Element) child, processed);
            }
        }
    }

    private Map<String, String> getNamespaces(Node node) {
        Map<String, String> namespaces = new HashMap<>();
        final String prefix = node.getPrefix();
        if (Strings.hasText(prefix) && "xmlns".equals(prefix) == false) {
            namespaces.put(prefix, node.getNamespaceURI());
        }
        final NamedNodeMap attributes = node.getAttributes();
        if (attributes != null) {
            for (int i = 0; i < attributes.getLength(); i++) {
                namespaces.putAll(getNamespaces(attributes.item(i)));
            }
        }
        return namespaces;
    }

    private void encryptElement(Element element, X509Certificate certificate, boolean storeKeyWithRetrievalMethod) throws Exception {
        // Save the parent node now, because it will change when we encrypt
        final Node parentNode = element.getParentNode();
        final Document document = element.getOwnerDocument();

        // Generate an AES key for the actual encryption
        final KeyGenerator aesGenerator = KeyGenerator.getInstance("AES");
        aesGenerator.init(randomFrom(supportedAesKeyLengths));
        final Key aesKey = aesGenerator.generateKey();

        // Encrypt the AES key with the public key of the recipient
        final XMLCipher keyCipher = XMLCipher.getInstance(randomFrom(XMLCipher.RSA_OAEP, XMLCipher.RSA_OAEP_11));
        keyCipher.init(XMLCipher.WRAP_MODE, certificate.getPublicKey());
        final EncryptedKey encryptedKey = keyCipher.encryptKey(document, aesKey);

        // Encryption context for actual content
        final XMLCipher xmlCipher = XMLCipher.getInstance(randomFrom(supportedAesTransformations));
        xmlCipher.init(XMLCipher.ENCRYPT_MODE, aesKey);

        final String keyElementId = randomId();

        // Include the key info for passing the AES key
        org.apache.xml.security.keys.KeyInfo keyInfo = new org.apache.xml.security.keys.KeyInfo(document);
        if (storeKeyWithRetrievalMethod) {
            keyInfo.addRetrievalMethod("#" + keyElementId, null, "http://www.w3.org/2001/04/xmlenc#EncryptedKey");
        } else {
            keyInfo.add(encryptedKey);
        }
        EncryptedData encryptedData = xmlCipher.getEncryptedData();
        encryptedData.setKeyInfo(keyInfo);

        // Include the content element itself
        // - The 3rd argument indicates whether to only encrypt the content (true) or the element itself (false)
        xmlCipher.doFinal(document, element, false);

        if (storeKeyWithRetrievalMethod) {
            final Element keyElement = buildEncryptedKeyElement(document, encryptedKey, certificate);
            keyElement.setAttribute("Id", keyElementId);
            keyElement.setIdAttribute("Id", true);
            parentNode.appendChild(keyElement);
        }
    }

    private Element buildEncryptedKeyElement(Document document, EncryptedKey encryptedKey, X509Certificate certificate)
        throws XMLSecurityException {
        final XMLCipher cipher = XMLCipher.getInstance();
        final org.apache.xml.security.keys.KeyInfo keyInfo = new org.apache.xml.security.keys.KeyInfo(document);
        final X509Data x509Data = new X509Data(document);
        x509Data.addCertificate(certificate);
        keyInfo.add(x509Data);
        encryptedKey.setKeyInfo(keyInfo);
        final EncryptionMethod method = cipher.createEncryptionMethod(XMLCipher.RSA_OAEP);
        method.setDigestAlgorithm(XMLCipher.SHA1);
        encryptedKey.setEncryptionMethod(method);
        return cipher.martial(document, encryptedKey);
    }

    private Response toResponse(String xml) throws SAXException, IOException, ParserConfigurationException {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        final Document doc = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        return authenticator.buildXmlObject(doc.getDocumentElement(), Response.class);
    }

    private String getStatusFailedResponse() {
        final Instant now = clock.instant();
        final String xml = "<?xml version='1.0' encoding='UTF-8'?>\n"
            + "<proto:Response"
            + "    Destination='%(SP_ACS_URL)' ID='%(randomId)' "
            + "    InResponseTo='%(requestId)'"
            + "    IssueInstant='%(now)'"
            + "    Version='2.0'"
            + "    xmlns:proto='urn:oasis:names:tc:SAML:2.0:protocol'"
            + "    xmlns:assert='urn:oasis:names:tc:SAML:2.0:assertion'"
            + "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + "    xmlns:xs='http://www.w3.org/2001/XMLSchema'"
            + "    xmlns:ds='http://www.w3.org/2000/09/xmldsig#' >"
            + "  <assert:Issuer>%(IDP_ENTITY_ID)</assert:Issuer>"
            + "  <proto:Status><proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:Requester'>"
            + "    <proto:StatusCode Value='urn:oasis:names:tc:SAML:2.0:status:InvalidNameIDPolicy'/></proto:StatusCode>"
            + "  </proto:Status>"
            + "</proto:Response>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", now);
        replacements.put("randomId", randomId());
        replacements.put("requestId", requestId);
        replacements.put("SP_ACS_URL", SP_ACS_URL);

        return NamedFormatter.format(xml, replacements);
    }

    private String getSimpleResponse(Instant now) {
        return getSimpleResponse(now, randomAlphaOfLengthBetween(12, 18), randomId());
    }

    private String getSimpleResponse(Instant now, String nameId, String sessionindex) {

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

    private String getResponseWithAudienceRestrictions(String... requiredAudiences) {
        String inner = Stream.of(requiredAudiences)
            .map(s -> "<assert:Audience>" + s + "</assert:Audience>")
            .collect(Collectors.joining());
        return getSimpleResponse(clock.instant()).replaceFirst("<assert:AuthnStatement",
            "<assert:Conditions><assert:AudienceRestriction>" + inner + "</assert:AudienceRestriction></assert:Conditions>" +
                "$0");
    }

    private String randomId() {
        return SamlUtils.generateSecureNCName(randomIntBetween(12, 36));
    }

    private SamlToken token(String content) {
        return token(content.getBytes(StandardCharsets.UTF_8));
    }

    private SamlToken token(byte[] content) {
        return new SamlToken(content, singletonList(requestId), null);
    }

}
