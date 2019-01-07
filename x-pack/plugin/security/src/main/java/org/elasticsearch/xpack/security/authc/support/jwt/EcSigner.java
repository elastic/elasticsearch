package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import java.security.Key;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;

public class EcSigner implements JwtSigner {

    private SignatureAlgorithm algorithm;
    private Key key;

    public EcSigner(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof ECPrivateKey == false) {
            throw new IllegalArgumentException("ECDSA signatures can only be created using a ECPrivateKey " +
                "but a [" + key.getClass().getName() + "] is provided");
        }
        if (SignatureAlgorithm.getEcAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for ECDSA signature");
        }
        this.algorithm = algorithm;
        this.key = key;
    }

    @Override
    public byte[] sign(byte[] data) {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }
        try {
            final Signature ecdsa = Signature.getInstance(algorithm.getJcaAlgoName());
            ecdsa.initSign((ECPrivateKey) key);
            ecdsa.update(data);
            return ecdsa.sign();
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to create the JWT RSA Signature", e);
        }
    }
}
