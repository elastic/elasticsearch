/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.util.ArrayUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class JwtUtil {
    // All JWSAlgorithm values in Family.HMAC_SHA, Family.RSA, and Family.EC (except ES256K)
    public static final JWSAlgorithm.Family SUPPORTED_JWS_ALGORITHMS = new JWSAlgorithm.Family(
        ArrayUtils.concat(
            JWSAlgorithm.Family.HMAC_SHA.toArray(new JWSAlgorithm[] {}),
            JWSAlgorithm.Family.RSA.toArray(new JWSAlgorithm[] {}),
            JWSAlgorithm.Family.EC.stream().filter(a -> (a.equals(JWSAlgorithm.ES256K) == false)).toArray(JWSAlgorithm[]::new)
        )
    );

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJWSVerifier(final Object secretKeyOrKeyPair) throws JOSEException {
        if (secretKeyOrKeyPair instanceof SecretKey hmacKey) {
            return getJwsSignerJWSVerifierTuple(hmacKey);
        } else if (secretKeyOrKeyPair instanceof KeyPair keyPair) {
            return getJwsSignerJWSVerifierTuple(keyPair);
        }
        throw new JOSEException(
            "Unsupported class ["
                + (secretKeyOrKeyPair == null ? "null" : secretKeyOrKeyPair.getClass().getCanonicalName())
                + "]. Supported classes are ["
                + SecretKey.class.getCanonicalName()
                + ", "
                + KeyPair.class.getCanonicalName()
                + "]."
        );
    }

    public static Tuple<JWSSigner, JWSVerifier> getJwsSignerJWSVerifierTuple(final KeyPair keyPair) throws JOSEException {
        final PrivateKey privateKey = keyPair.getPrivate();
        if (privateKey instanceof RSAPrivateKey rsaPrivateKey) {
            return new Tuple<>(new RSASSASigner(rsaPrivateKey), new RSASSAVerifier((RSAPublicKey) keyPair.getPublic()));
        } else if (privateKey instanceof ECPrivateKey ecPrivateKey) {
            return new Tuple<>(new ECDSASigner(ecPrivateKey), new ECDSAVerifier((ECPublicKey) keyPair.getPublic()));
        }
        throw new JOSEException(
            "Unsupported class ["
                + (privateKey == null ? "null" : privateKey.getClass().getCanonicalName())
                + "]. Supported classes are ["
                + RSAPrivateKey.class.getCanonicalName()
                + ", "
                + ECPrivateKey.class.getCanonicalName()
                + "]."
        );
    }

    public static Tuple<JWSSigner, JWSVerifier> getJwsSignerJWSVerifierTuple(final SecretKey hmacKey) throws JOSEException {
        return new Tuple<>(new MACSigner(hmacKey), new MACVerifier(hmacKey));
    }

    public static Object generateSecretKeyOrKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JWSAlgorithm.Family.HMAC_SHA.contains(jwsAlgorithm)) {
            return generateSecretKey(signatureAlgorithm); // SecretKeySpec
        } else if (JWSAlgorithm.Family.RSA.contains(jwsAlgorithm)) {
            return generateRsaKeyPair(signatureAlgorithm); // KeyPair(RSAPublicKey,RSAPrivateKey)
        } else if (JWSAlgorithm.Family.EC.contains(jwsAlgorithm)) {
            return generateEcKeyPair(signatureAlgorithm); // KeyPair(ECPublicKey,ECPrivateKey)
        }
        throw new JOSEException(
            "Unsupported signature algorithm ["
                + signatureAlgorithm
                + "]. Supported signature algorithms are "
                + SUPPORTED_JWS_ALGORITHMS
                + "."
        );
    }

    // Nimbus JOSE+JWT requires HMAC keys to match or exceed digest strength.
    public static SecretKey generateSecretKey(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JWSAlgorithm.Family.HMAC_SHA.contains(jwsAlgorithm)) {
            // HMAC key lengths limited by specific SHA-2 lengths.
            final int minRequiredSecretBytesLength = MACSigner.getMinRequiredSecretLength(jwsAlgorithm) / 8;
            final byte[] hmacKeyBytes = new byte[JwtTestCase.randomIntBetween(
                minRequiredSecretBytesLength,
                minRequiredSecretBytesLength * 3
            )];
            JwtTestCase.random().nextBytes(hmacKeyBytes);
            return new SecretKeySpec(hmacKeyBytes, signatureAlgorithm);
        }
        throw new JOSEException(
            "Unsupported signature algorithm "
                + signatureAlgorithm
                + "]. Supported signature algorithms are "
                + JWSAlgorithm.Family.HMAC_SHA
                + "."
        );
    }

    public static KeyPair generateKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JWSAlgorithm.Family.RSA.contains(jwsAlgorithm)) {
            return generateRsaKeyPair(signatureAlgorithm);
        } else if (JWSAlgorithm.Family.EC.contains(jwsAlgorithm)) {
            return generateEcKeyPair(signatureAlgorithm);
        }
        throw new JOSEException(
            "Unsupported signature algorithm "
                + signatureAlgorithm
                + "]. Supported signature algorithms are "
                + SUPPORTED_JWS_ALGORITHMS
                + "."
        );
    }

    // Nimbus JOSE+JWT requires RSA keys to match or exceed 2048-bit strength. No dependency on digest strength.
    public static KeyPair generateRsaKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JWSAlgorithm.Family.RSA.contains(jwsAlgorithm)) {
            // RSA key pair lengths not limited by specific SHA-2 lengths.
            final Integer rsaSize = JwtTestCase.randomFrom(2048, 3072);
            final RSAKey rsaKeyPair = new RSAKeyGenerator(rsaSize, false).generate();
            return new KeyPair(rsaKeyPair.toPublicKey(), rsaKeyPair.toPrivateKey());
        }
        throw new JOSEException(
            "Unsupported signature algorithm "
                + signatureAlgorithm
                + "]. Supported signature algorithms are "
                + JWSAlgorithm.Family.RSA
                + "."
        );
    }

    // Nimbus JOSE+JWT requires EC curves to match digest strength (as per RFC 7519).
    public static KeyPair generateEcKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JWSAlgorithm.Family.EC.contains(jwsAlgorithm)) {
            // EC key pair curves limited by specific SHA-2 lengths.
            final Curve ecCurve = JwtTestCase.randomFrom(Curve.forJWSAlgorithm(jwsAlgorithm));
            final ECKey ecKeyPair = new ECKeyGenerator(ecCurve).generate();
            return new KeyPair(ecKeyPair.toPublicKey(), ecKeyPair.toPrivateKey());
        }
        throw new JOSEException(
            "Unsupported signature algorithm "
                + signatureAlgorithm
                + "]. Supported signature algorithms are "
                + JWSAlgorithm.Family.EC
                + "."
        );
    }

    public static SignedJWT generateValidSignedJWT(final JWSSigner jwsSigner, final String signatureAlgorithm) throws Exception {
        final Tuple<JWSHeader, JWTClaimsSet> headerAndBody = createJwsHeaderAndJwtClaimsSet(
            signatureAlgorithm,
            JwtTestCase.randomFrom("https://www.example.com/", "") + "iss1" + JwtTestCase.randomIntBetween(0, 99),
            JwtTestCase.randomFrom(List.of("rp_client1"), List.of("aud1", "aud2", "aud3")),
            JwtTestCase.randomFrom("sub", "uid", "name", "dn", "email", "custom"),
            "principal1",
            JwtTestCase.randomBoolean() ? null : JwtTestCase.randomFrom("groups", "roles", "other"),
            JwtTestCase.randomFrom(List.of(""), List.of("grp1"), List.of("rol1", "rol2", "rol3"), List.of("per1"))
        );
        return signSignedJwt(jwsSigner, headerAndBody.v1(), headerAndBody.v2());
    }

    public static SignedJWT signSignedJwt(final JWSSigner jwtSigner, final JWSHeader jwsHeader, final JWTClaimsSet jwtClaimsSet)
        throws JOSEException {
        final SignedJWT signedJwt = new SignedJWT(jwsHeader, jwtClaimsSet);
        signedJwt.sign(jwtSigner);
        return signedJwt;
    }

    public static boolean verifySignedJWT(final JWSVerifier jwtVerifier, final SignedJWT signedJwt) throws Exception {
        return signedJwt.verify(jwtVerifier);
    }

    public static Tuple<JWSHeader, JWTClaimsSet> createJwsHeaderAndJwtClaimsSet(
        final String signatureAlgorithm,
        final String issuer,
        final List<String> audiences,
        final String principalClaimName,
        final String principalClaimValue,
        final String groupsClaimName,
        final List<String> groupsClaimValue
    ) {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        final JWSHeader jwtHeader = new JWSHeader.Builder(jwsAlgorithm).build();
        final JWTClaimsSet.Builder jwtClaimsSetBuilder = new JWTClaimsSet.Builder().jwtID(
            JwtTestCase.randomFrom((String) null, JwtTestCase.randomAlphaOfLengthBetween(1, 20))
        )
            .issueTime(JwtTestCase.randomFrom((Date) null, Date.from(Instant.now().minusSeconds(JwtTestCase.randomLongBetween(1, 60)))))
            .notBeforeTime(JwtTestCase.randomFrom((Date) null, Date.from(Instant.now().minusSeconds(JwtTestCase.randomLongBetween(1, 60)))))
            .expirationTime(
                JwtTestCase.randomFrom((Date) null, Date.from(Instant.now().plusSeconds(JwtTestCase.randomLongBetween(3600, 7200))))
            )
            .issuer(issuer)
            .audience(audiences)
            // .subject(subject)
            .claim("nonce", new Nonce());
        if ((Strings.hasText(principalClaimName)) && (principalClaimValue != null)) {
            jwtClaimsSetBuilder.claim(principalClaimName, principalClaimValue.toString());
        }
        if ((Strings.hasText(groupsClaimName)) && (groupsClaimValue != null)) {
            jwtClaimsSetBuilder.claim(groupsClaimName, groupsClaimValue.toString());
        }
        final JWTClaimsSet jwtClaimsSet = jwtClaimsSetBuilder.build();
        final Tuple<JWSHeader, JWTClaimsSet> headerAndBody = new Tuple<JWSHeader, JWTClaimsSet>(jwtHeader, jwtClaimsSet);
        return headerAndBody;
    }
}
