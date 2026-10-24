/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.x500.X500Principal;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The security CLI is loaded by {@code CliToolProvider} into a child class loader whose parent is the launcher's class path
 * ({@code lib/*.jar}). In a FIPS deployment that parent holds the Bouncy Castle FIPS jars, which define the same
 * {@code org.bouncycastle} packages as the Bouncy Castle used by this module. With parent-first delegation the module's own classes
 * are shadowed by the FIPS ones; before Bouncy Castle was shaded this failed to link (the unshaded {@code BouncyCastleProvider}
 * threw a {@code NoSuchMethodError} against the FIPS {@code CryptoServicesRegistrar}).
 * <p>
 * This test reproduces that arrangement: the facade runs from a child loader that holds the shaded jar but no unshaded Bouncy Castle,
 * and the BC FIPS provider jar sits on the parent. The JVM keeps its normal security configuration, so this covers the class loading
 * aspect only, not FIPS approved-only behaviour. Because the child loader has no unshaded Bouncy Castle, the test also fails if the
 * module ever references an unshaded {@code org.bouncycastle} class, which the other tests cannot detect as they have unshaded Bouncy
 * Castle on their class path.
 * </p>
 */
public class ShadedBouncyCastleTests extends ESTestCase {

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    public void testFacadeWorksWithBcFipsOnParentClassPath() throws Exception {
        final String bcFipsJar = System.getProperty("tests.bcfips.jar");
        assertThat("tests.bcfips.jar is set by build.gradle", bcFipsJar, notNullValue());
        final URL[] parentUrls = { PathUtils.get(bcFipsJar).toUri().toURL() };

        final List<URL> childUrls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(System.getProperty("path.separator"))) {
            final Path path = PathUtils.get(entry);
            final String name = path.getFileName().toString();
            // the unshaded Bouncy Castle is a test dependency, the tool does not ship it; the randomized testing framework is left out
            // because the copy of Randomness in the child loader would otherwise look for the test context of this thread
            if (name.startsWith("bcprov-")
                || name.startsWith("bcpkix-")
                || name.startsWith("bcutil-")
                || name.contains("randomizedtesting")) {
                continue;
            }
            childUrls.add(path.toUri().toURL());
        }

        try (
            URLClassLoader parent = new URLClassLoader(parentUrls, ClassLoader.getPlatformClassLoader());
            URLClassLoader child = URLClassLoader.newInstance(childUrls.toArray(URL[]::new), parent)
        ) {
            // the FIPS jar shadows the org.bouncycastle packages for the child, as it does for the real tool
            assertThat(Class.forName("org.bouncycastle.asn1.x509.GeneralNames", false, child).getClassLoader(), sameInstance(parent));

            final Class<?> certGenUtils = Class.forName(CertGenUtils.class.getName(), true, child);
            assertThat(certGenUtils.getClassLoader(), sameInstance(child));
            final KeyPair keyPair = (KeyPair) certGenUtils.getMethod("generateKeyPair", int.class).invoke(null, 2048);
            final Class<?> subjectAlternativeNames = Class.forName(SubjectAlternativeNames.class.getName(), true, child);
            final X509Certificate certificate = (X509Certificate) certGenUtils.getMethod(
                "generateSignedCertificate",
                X500Principal.class,
                subjectAlternativeNames,
                KeyPair.class,
                X509Certificate.class,
                PrivateKey.class,
                int.class
            ).invoke(null, new X500Principal("CN=test"), null, keyPair, null, null, 30);
            certificate.verify(keyPair.getPublic());

            // password protected PEM output, read back with the unshaded Bouncy Castle of this test's own class loader
            final char[] password = randomAlphaOfLengthBetween(8, 32).toCharArray();
            final Class<?> pemWriter = Class.forName(PemWriter.class.getName(), true, child);
            final Class<?> pemEncryptor = Class.forName(PemEncryptor.class.getName(), true, child);
            final StringWriter out = new StringWriter();
            try (Closeable writer = (Closeable) pemWriter.getConstructor(Writer.class).newInstance(out)) {
                final Object encryptor = pemEncryptor.getConstructor(String.class, char[].class).newInstance("AES-128-CBC", password);
                pemWriter.getMethod("writeObject", PrivateKey.class, pemEncryptor).invoke(writer, keyPair.getPrivate(), encryptor);
            }
            try (PEMParser parser = new PEMParser(new StringReader(out.toString()))) {
                final Object object = parser.readObject();
                assertThat(object, instanceOf(PEMEncryptedKeyPair.class));
                final byte[] encoded = ((PEMEncryptedKeyPair) object).decryptKeyPair(new BcPEMDecryptorProvider(password))
                    .getPrivateKeyInfo()
                    .getEncoded();
                assertThat(encoded, equalTo(keyPair.getPrivate().getEncoded()));
            }
        }
    }
}
