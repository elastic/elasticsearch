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
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

/**
 * A PGP signature verifier that uses Bouncy Castle implementation.
 * <p>
 * This implementation was lifted from <code>InstallPluginAction</code> to isolate Bouncy Castle usage.
 * </p>
 */
public class PgpSignatureVerifier {

    /**
     * @param publicKeyId the public key ID of the signing key that is expected to have signed the official plugin.
     * @param urlString the URL source of the downloaded plugin ZIP
     * @param pluginZipInputStream an input stream to the raw bytes of the plugin ZIP.
     * @param ascInputStream the URL source of the PGP signature for the downloaded plugin ZIP
     * @param publicKeyInputStream an input stream to the public key of the signing key.
     */
    public static void verifySignature(
        String publicKeyId,
        String urlString,
        InputStream pluginZipInputStream,
        InputStream ascInputStream,
        InputStream publicKeyInputStream
    ) throws IOException {

        try (
            // fin is a file stream over the downloaded plugin zip whose signature to verify
            InputStream fin = pluginZipInputStream;
            // sin is a URL stream to the signature corresponding to the downloaded plugin zip
            InputStream sin = ascInputStream;
            // ain is a input stream to the public key in ASCII-Armor format (RFC4880)
            InputStream ain = new ArmoredInputStream(publicKeyInputStream)
        ) {
            final JcaPGPObjectFactory factory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(sin));
            final PGPSignature signature = ((PGPSignatureList) factory.nextObject()).get(0);

            // validate the signature has key ID matching our public key ID
            final String keyId = Long.toHexString(signature.getKeyID()).toUpperCase(Locale.ROOT);
            if (publicKeyId.equals(keyId) == false) {
                throw new IllegalStateException("key id [" + keyId + "] does not match expected key id [" + publicKeyId + "]");
            }

            // compute the signature of the downloaded plugin zip
            computeSignatureForDownloadedPlugin(fin, ain, signature);

            // finally we verify the signature of the downloaded plugin zip matches the expected signature
            if (signature.verify() == false) {
                throw new IllegalStateException("signature verification for [" + urlString + "] failed");
            }
        } catch (PGPException e) {
            throw new IOException("PGP exception during signature verification for [" + urlString + "]", e);
        }
    }

    private static void computeSignatureForDownloadedPlugin(InputStream fin, InputStream ain, PGPSignature signature) throws PGPException,
        IOException {
        final PGPPublicKeyRingCollection collection = new PGPPublicKeyRingCollection(ain, new JcaKeyFingerprintCalculator());
        final PGPPublicKey key = collection.getPublicKey(signature.getKeyID());
        signature.init(new JcaPGPContentVerifierBuilderProvider(), key);
        final byte[] buffer = new byte[1024];
        int read;
        while ((read = fin.read(buffer)) != -1) {
            signature.update(buffer, 0, read);
        }
    }

}
