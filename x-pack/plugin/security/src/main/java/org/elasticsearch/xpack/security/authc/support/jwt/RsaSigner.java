package org.elasticsearch.xpack.security.authc.support.jwt;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.PrivateKey;
import java.security.Signature;

/**
 * Class offering necessary functionality for signing JWTs with a Private Key using
 * RSASSA-PKCS1-v1_5 (PKCS#1)
 */
public class RsaSigner implements JwtSigner {

    private SignatureAlgorithm algorithm;
    private Key key;

    public RsaSigner(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof PrivateKey == false) {
            throw new IllegalArgumentException("RSA signatures can only be created using a PrivateKey " +
                "but a [" + key.getClass().getName() + "] is provided");
        }
        if (SignatureAlgorithm.getRsaAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for RSA signature");
        }
        this.algorithm = algorithm;
        this.key = key;
    }

    /**
     * Signs the data byte array with an Private Key using RSASSA-PKCS1-v1_5 (PKCS#1)
     *
     * @param data the data to be signed
     * @return the signature bytes
     * @throws GeneralSecurityException if any error was encountered while signing
     */
    @Override
    public byte[] sign(byte[] data) throws GeneralSecurityException {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }

        final Signature rsa = Signature.getInstance(algorithm.getJcaAlgoName());
        rsa.initSign((PrivateKey) key);
        rsa.update(data);
        return rsa.sign();
    }
}
