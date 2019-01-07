package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import java.security.Key;
import java.security.PrivateKey;
import java.security.Signature;

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

    @Override
    public byte[] sign(byte[] data) {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }
        try {
            final Signature rsa = Signature.getInstance(algorithm.getJcaAlgoName());
            rsa.initSign((PrivateKey) key);
            rsa.update(data);
            return rsa.sign();
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to create the JWT RSA Signature", e);
        }
    }
}
