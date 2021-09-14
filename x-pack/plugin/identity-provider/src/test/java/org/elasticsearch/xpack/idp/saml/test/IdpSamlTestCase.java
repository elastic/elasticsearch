/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.FileMatchers;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.support.XmlValidator;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.core.xml.io.UnmarshallerFactory;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.security.x509.X509Credential;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import static org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport.getUnmarshallerFactory;

public abstract class IdpSamlTestCase extends ESTestCase {

    private static Locale restoreLocale;

    @BeforeClass
    public static void localeChecks() throws Exception {
        Logger logger = LogManager.getLogger(IdpSamlTestCase.class);
        SamlInit.initialize();
        if (isTurkishLocale()) {
            // See: https://github.com/elastic/elasticsearch/issues/29824
            logger.warn("Attempting to run SAML test on turkish-like locale, but that breaks OpenSAML. Switching to English.");
            restoreLocale = Locale.getDefault();
            Locale.setDefault(Locale.ENGLISH);
        }
    }

    private static boolean isTurkishLocale() {
        return Locale.getDefault().getLanguage().equals(new Locale("tr").getLanguage())
            || Locale.getDefault().getLanguage().equals(new Locale("az").getLanguage());
    }

    @AfterClass
    public static void restoreLocale() {
        if (restoreLocale != null) {
            Locale.setDefault(restoreLocale);
            restoreLocale = null;
        }
    }

    @SuppressWarnings("unchecked")
    protected static void mockRegisteredServiceProvider(SamlIdentityProvider idp, String entityId, SamlServiceProvider sp) {
        Mockito.doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(4));
            assertThat(args[0], Matchers.equalTo(entityId));
            assertThat(args[args.length - 1], Matchers.instanceOf(ActionListener.class));
            ActionListener<SamlServiceProvider> listener = (ActionListener<SamlServiceProvider>) args[args.length - 1];

            listener.onResponse(sp);
            return null;
        }).when(idp).resolveServiceProvider(Mockito.eq(entityId), Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    protected static void mockRegisteredServiceProvider(SamlServiceProviderResolver resolverMock, String entityId,
                                                        SamlServiceProvider sp) {
        Mockito.doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(2));
            assertThat(args[0], Matchers.equalTo(entityId));
            assertThat(args[args.length-1], Matchers.instanceOf(ActionListener.class));
            ActionListener<SamlServiceProvider> listener = (ActionListener<SamlServiceProvider>) args[args.length-1];

            listener.onResponse(sp);
            return null;
        }).when(resolverMock).resolve(Mockito.eq(entityId), Mockito.any(ActionListener.class));
    }

    protected List<X509Credential> readCredentials() throws GeneralSecurityException, IOException {
        List<X509Credential> list = new ArrayList<>(2);
        list.add(readCredentials("RSA", 1024));
        list.add(readCredentials("RSA", 2048));
        Collections.shuffle(list, random());
        return list;
    }

    protected X509Credential readCredentials(String type, int size) throws GeneralSecurityException, IOException {
        Path certPath = getDataPath("/keypair/keypair_" + type + "_" + size + ".crt");
        Path keyPath = getDataPath("/keypair/keypair_" + type + "_" + size + ".key");
        assertThat(certPath, FileMatchers.isRegularFile());
        assertThat(keyPath, FileMatchers.isRegularFile());

        final X509Certificate[] certificates = CertParsingUtils.readX509Certificates(List.of(certPath));
        assertThat("Incorrect number of certificates in " + certPath, certificates, Matchers.arrayWithSize(1));

        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> new char[0]);
        return new BasicX509Credential(certificates[0], privateKey);
    }

    protected <T extends XMLObject> T domElementToXmlObject(Element element, Class<T> type) throws UnmarshallingException {
        final UnmarshallerFactory unmarshallerFactory = getUnmarshallerFactory();
        Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(element);
        assertThat(unmarshaller, Matchers.notNullValue());
        final XMLObject object = unmarshaller.unmarshall(element);
        assertThat(object, Matchers.instanceOf(type));
        return type.cast(object);
    }

    protected void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = new SamlFactory().getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    protected String toString(XMLObject object) {
        try {
            return toString(XMLObjectSupport.marshall(object));
        } catch (MarshallingException e) {
            throw new RuntimeException("cannot marshall XML object to DOM", e);
        }
    }

    protected String toString(Element element) {
        try (StringWriter writer = new StringWriter()) {
            print(element, writer, true);
            return writer.toString();
        } catch (TransformerException e) {
            throw new RuntimeException("cannot transform XML element to string", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void assertValidXml(String xml) throws Exception {
        new XmlValidator( "saml-schema-metadata-2.0.xsd").validate(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    protected String joinCertificateLines(String... lines) {
        return Arrays.stream(lines).collect(Collectors.joining(System.lineSeparator()));
    }

    protected String normaliseXml(String input) {
        // Remove spaces between elements, and compress other spaces. These patterns don't use \s because
        // that would match newlines.
        return input.replaceAll("> +<", "><").replaceAll(" +", " ");
    }
}
