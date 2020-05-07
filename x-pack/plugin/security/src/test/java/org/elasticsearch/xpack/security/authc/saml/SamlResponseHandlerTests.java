/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.xml.security.Init;
import org.apache.xml.security.encryption.XMLCipher;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringReader;
import java.security.KeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
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

import static java.util.Collections.singletonList;
import static javax.xml.crypto.dsig.CanonicalizationMethod.EXCLUSIVE;
import static javax.xml.crypto.dsig.Transform.ENVELOPED;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML20_NS;

public class SamlResponseHandlerTests extends SamlTestCase {
    protected static final String SP_ENTITY_ID = "https://sp.saml.elastic.test/";
    protected static final String IDP_ENTITY_ID = "https://idp.saml.elastic.test/";
    protected static final String SP_ACS_URL = SP_ENTITY_ID + "sso/post";
    protected static final String SP_LOGOUT_URL = SP_ENTITY_ID + "sso/logout";
    protected static Tuple<X509Certificate, PrivateKey> idpSigningCertificatePair;
    protected static Tuple<X509Certificate, PrivateKey> spSigningCertificatePair;
    protected static List<Tuple<X509Certificate, PrivateKey>> spEncryptionCertificatePairs;
    protected static List<Integer> supportedAesKeyLengths;
    protected static List<String> supportedAesTransformations;
    protected ClockMock clock;
    protected String requestId;
    protected TimeValue maxSkew;

    @BeforeClass
    public static void init() throws Exception {
        SamlUtils.initialize(LogManager.getLogger(SamlResponseHandlerTests.class));
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
        idpSigningCertificatePair = readRandomKeyPair(SamlResponseHandlerTests.randomSigningAlgorithm());
        spSigningCertificatePair = readRandomKeyPair(SamlResponseHandlerTests.randomSigningAlgorithm());
        spEncryptionCertificatePairs = Arrays.asList(readKeyPair("RSA_2048"), readKeyPair("RSA_4096"));
    }

    protected static String randomSigningAlgorithm() {
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

    private static KeyInfo getKeyInfo(XMLSignatureFactory factory, X509Certificate certificate) throws KeyException {
        KeyInfoFactory kif = factory.getKeyInfoFactory();
        javax.xml.crypto.dsig.keyinfo.X509Data data = kif.newX509Data(Collections.singletonList(certificate));
        return kif.newKeyInfo(singletonList(data));
    }

    protected SpConfiguration getSpConfiguration(List<String> reqAuthnCtxClassRef) {
        final SigningConfiguration signingConfiguration = new SigningConfiguration(
            Collections.singleton("*"),
                (X509Credential) buildOpenSamlCredential(spSigningCertificatePair).get(0));
        final List<X509Credential> spEncryptionCredentials = buildOpenSamlCredential(spEncryptionCertificatePairs).stream()
                .map((cred) -> (X509Credential) cred).collect(Collectors.<X509Credential>toList());
        return new SpConfiguration(SP_ENTITY_ID, SP_ACS_URL, SP_LOGOUT_URL, signingConfiguration, spEncryptionCredentials,
            reqAuthnCtxClassRef);
    }

    protected IdpConfiguration getIdpConfiguration(Supplier<List<Credential>> credentials) {
        return new IdpConfiguration(IDP_ENTITY_ID, credentials);
    }

    protected String randomId() {
        return SamlUtils.generateSecureNCName(randomIntBetween(12, 36));
    }

    protected String signDoc(String xml) throws Exception {
        return signDoc(xml, EXCLUSIVE, SamlResponseHandlerTests.idpSigningCertificatePair);
    }

    protected String signDoc(String xml, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        return signDoc(xml, EXCLUSIVE, keyPair);
    }

    protected String signDoc(String xml, String c14nMethod) throws Exception {
        return signDoc(xml, c14nMethod, SamlResponseHandlerTests.idpSigningCertificatePair);
    }

    private String signDoc(String xml, String c14nMethod, Tuple<X509Certificate, PrivateKey> keyPair) throws Exception {
        final Document doc = parseDocument(xml);
        signElement(doc.getDocumentElement(), keyPair, c14nMethod);
        return SamlUtils.toString(doc.getDocumentElement());
    }

    protected Document parseDocument(String xml) throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        final DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
        return documentBuilder.parse(new InputSource(new StringReader(xml)));
    }

    /**
     * Randomly selects digital signature algorithm URI for given private key
     * algorithm ({@link PrivateKey#getAlgorithm()}).
     *
     * @param key
     *            {@link PrivateKey}
     * @return algorithm URI
     */
    protected String getSignatureAlgorithmURI(PrivateKey key) {
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

    protected void signElement(Element parent, Tuple<X509Certificate, PrivateKey> keyPair, String c14nMethod) throws Exception {
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

        final KeyInfo keyInfo = SamlResponseHandlerTests.getKeyInfo(fac, certificate);

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
}
