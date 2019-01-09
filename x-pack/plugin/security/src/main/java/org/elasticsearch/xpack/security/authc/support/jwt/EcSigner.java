package org.elasticsearch.xpack.security.authc.support.jwt;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.ECPrivateKey;

public class EcSigner implements JwtSigner {

    private SignatureAlgorithm algorithm;
    private Key key;
    private int signatureLength;

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
        this.signatureLength = getSignatureLength(algorithm);
    }

    private int getSignatureLength(SignatureAlgorithm algorithm) {
        switch (algorithm) {
            case ES256:
                return 64;
            case ES384:
                return 96;
            case ES512:
                return 132;
            default:
                throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for ECDSA signature");
        }
    }


    @Override
    public byte[] sign(byte[] data) throws GeneralSecurityException {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }

        final Signature ecdsa = Signature.getInstance(algorithm.getJcaAlgoName());
        ecdsa.initSign((ECPrivateKey) key);
        ecdsa.update(data);
        return convertToJose(ecdsa.sign());
    }

    /**
     * Converts a DER Encoded signature to JOSE so that it can be attached to a JWT. See
     * <a href="https://tools.ietf.org/html/rfc7518#section-3.4">the specification</a>
     * Based on https://github.com/jwtk/jjwt/blob/1520ae8a21052b376282f8a38d310a91b15285e5/impl/src/main/java/io/jsonwebtoken/impl/crypto/EllipticCurveProvider.java
     *
     * @param derSignature The DER formatted signature
     * @return
     * @throws SignatureException
     */
    private byte[] convertToJose(byte[] derSignature) throws SignatureException {
        if (derSignature.length < 8 || derSignature[0] != 48) {
            throw new SignatureException("Invalid DER encoded ECDSA signature");
        }

        int offset;
        if (derSignature[1] > 0) {
            offset = 2;
        } else if (derSignature[1] == (byte) 0x81) {
            offset = 3;
        } else {
            throw new SignatureException("Invalid DER encoded ECDSA signature");
        }

        byte rLength = derSignature[offset + 1];

        int i = rLength;
        while ((i > 0) && (derSignature[(offset + 2 + rLength) - i] == 0)) {
            i--;
        }

        byte sLength = derSignature[offset + 2 + rLength + 1];

        int j = sLength;
        while ((j > 0) && (derSignature[(offset + 2 + rLength + 2 + sLength) - j] == 0)) {
            j--;
        }

        int rawLen = Math.max(i, j);
        rawLen = Math.max(rawLen, signatureLength / 2);

        if ((derSignature[offset - 1] & 0xff) != derSignature.length - offset
            || (derSignature[offset - 1] & 0xff) != 2 + rLength + 2 + sLength
            || derSignature[offset] != 2
            || derSignature[offset + 2 + rLength] != 2) {
            throw new SignatureException("Invalid DER encoded ECDSA signature");
        }

        final byte[] jwtSignature = new byte[2 * rawLen];

        System.arraycopy(derSignature, (offset + 2 + rLength) - i, jwtSignature, rawLen - i, i);
        System.arraycopy(derSignature, (offset + 2 + rLength + 2 + sLength) - j, jwtSignature, 2 * rawLen - j, j);

        return jwtSignature;
    }
}
