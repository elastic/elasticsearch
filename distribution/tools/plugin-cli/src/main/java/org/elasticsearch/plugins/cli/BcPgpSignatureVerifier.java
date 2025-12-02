/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A PGP signature verifier that uses Bouncy Castle implementation.
 * This implementation was lifted from {@link InstallPluginAction} to isolate Bouncy Castle usage.
 */
public class BcPgpSignatureVerifier implements PgpSignatureVerifier {

    private final Terminal terminal;

    public BcPgpSignatureVerifier(Terminal terminal) {
        this.terminal = terminal;
    }

    /**
     * Verify the signature of the downloaded plugin ZIP. The signature is obtained from the source of the downloaded plugin by appending
     * ".asc" to the URL. It is expected that the plugin is signed with the Elastic signing key with ID D27D666CD88E42B4.
     *
     * @param zip       the path to the downloaded plugin ZIP
     * @param urlString the URL source of the downloaded plugin ZIP
     * @param ascInputStream the URL source of the PGP signature for the downloaded plugin ZIP
     * @throws IOException  if an I/O exception occurs reading from various input streams or if the PGP implementation throws an
     * internal exception during verification
     */
    @Override
    public void verifySignature(final Path libDir, final Path zip, final String urlString, final InputStream ascInputStream)
        throws IOException {

        try (
            // fin is a file stream over the downloaded plugin zip whose signature to verify
            InputStream fin = pluginZipInputStream(zip);
            // sin is a URL stream to the signature corresponding to the downloaded plugin zip
            InputStream sin = ascInputStream;
            // ain is a input stream to the public key in ASCII-Armor format (RFC4880)
            InputStream ain = new ArmoredInputStream(getPublicKey())
        ) {
            final JcaPGPObjectFactory factory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(sin));
            final PGPSignature signature = ((PGPSignatureList) factory.nextObject()).get(0);

            // validate the signature has key ID matching our public key ID
            final String keyId = Long.toHexString(signature.getKeyID()).toUpperCase(Locale.ROOT);
            if (getPublicKeyId().equals(keyId) == false) {
                throw new IllegalStateException("key id [" + keyId + "] does not match expected key id [" + getPublicKeyId() + "]");
            }

            // compute the signature of the downloaded plugin zip, wrapped with long execution warning
            timedComputeSignatureForDownloadedPlugin(fin, ain, signature);

            // finally we verify the signature of the downloaded plugin zip matches the expected signature
            if (signature.verify() == false) {
                throw new IllegalStateException("signature verification for [" + urlString + "] failed");
            }
        } catch (PGPException e) {
            throw new IOException("PGP exception during signature verification for [" + urlString + "]", e);
        }
    }

    private void timedComputeSignatureForDownloadedPlugin(InputStream fin, InputStream ain, PGPSignature signature) throws PGPException,
        IOException {
        final Timer timer = new Timer();

        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    reportLongSignatureVerification();
                }
            }, acceptableSignatureVerificationDelay());

            computeSignatureForDownloadedPlugin(fin, ain, signature);
        } finally {
            timer.cancel();
        }
    }

    // package private for testing
    void computeSignatureForDownloadedPlugin(InputStream fin, InputStream ain, PGPSignature signature) throws PGPException, IOException {
        final PGPPublicKeyRingCollection collection = new PGPPublicKeyRingCollection(ain, new JcaKeyFingerprintCalculator());
        final PGPPublicKey key = collection.getPublicKey(signature.getKeyID());
        signature.init(new JcaPGPContentVerifierBuilderProvider().setProvider(new BouncyCastleFipsProvider()), key);
        final byte[] buffer = new byte[1024];
        int read;
        while ((read = fin.read(buffer)) != -1) {
            signature.update(buffer, 0, read);
        }
    }

    // package private for testing
    void reportLongSignatureVerification() {
        terminal.println(
            "The plugin installer is trying to verify the signature of the downloaded plugin "
                + "but this verification is taking longer than expected. This is often because the "
                + "plugin installer is waiting for your system to supply it with random numbers. "
                + ((System.getProperty("os.name").startsWith("Windows") == false)
                    ? "Ensure that your system has sufficient entropy so that reads from /dev/random do not block."
                    : "")
        );
    }

    // package private for testing
    long acceptableSignatureVerificationDelay() {
        return 5_000;
    }

    /**
     * An input stream to the raw bytes of the plugin ZIP.
     *
     * @param zip the path to the downloaded plugin ZIP
     * @return an input stream to the raw bytes of the plugin ZIP.
     * @throws IOException if an I/O exception occurs preparing the input stream
     */
    InputStream pluginZipInputStream(final Path zip) throws IOException {
        return Files.newInputStream(zip);
    }

    /**
     * Return the public key ID of the signing key that is expected to have signed the official plugin.
     *
     * @return the public key ID
     */
    String getPublicKeyId() {
        return "D27D666CD88E42B4";
    }

    /**
     * An input stream to the public key of the signing key.
     *
     * @return an input stream to the public key
     */
    InputStream getPublicKey() {
        return InstallPluginAction.class.getResourceAsStream("/public_key.asc");
    }

}
