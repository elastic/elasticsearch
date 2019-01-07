package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

public class HmacSigner implements JwtSigner {

    private Key key;
    private SignatureAlgorithm algorithm;

    public HmacSigner(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof SecretKey == false) {
            throw new IllegalArgumentException("HMAC signatures can only be created using a SecretKey but a [" + key.getClass().getName() +
                "] is provided");
        }
        if (SignatureAlgorithm.getHmacAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for HMAC signature");
        }
        this.key = key;
        this.algorithm = algorithm;

    }

    @Override
    public byte[] sign(byte[] data) {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }

        try {
            final SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), algorithm.getJcaAlgoName());
            final Mac mac = Mac.getInstance(algorithm.getJcaAlgoName());
            mac.init(keySpec);
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to create the JWT HMAC Signature", e);
        }
    }
}
