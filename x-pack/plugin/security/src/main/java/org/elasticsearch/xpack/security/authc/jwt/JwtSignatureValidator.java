/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.List;
import java.util.stream.Stream;

public interface JwtSignatureValidator extends Releasable {

    @Override
    default void close() {}

    void validate(String tokenPrincipal, SignedJWT jwt, ActionListener<Void> listener);

    boolean hasUsableJwksAlgs();

    class FailingJwtSignatureValidator implements JwtSignatureValidator {

        private FailingJwtSignatureValidator() {}

        @Override
        public void validate(String tokenPrincipal, SignedJWT jwt, ActionListener<Void> listener) {
            listener.onFailure(new ElasticsearchSecurityException("signature validation failure"));
        }

        @Override
        public boolean hasUsableJwksAlgs() {
            return false;
        }
    }

    FailingJwtSignatureValidator FAILING_JWT_SIGNATURE_VALIDATOR = new FailingJwtSignatureValidator();

    class DelegatingJwtSignatureValidator implements JwtSignatureValidator {

        private static final Logger logger = LogManager.getLogger(DelegatingJwtSignatureValidator.class);

        private final RealmConfig realmConfig;
        final List<String> allowedJwksAlgsPkc;
        final List<String> allowedJwksAlgsHmac;
        private final JwtSignatureValidator hmacJwtSignatureValidator;
        private final JwtSignatureValidator pkcJwtSignatureValidator;

        public DelegatingJwtSignatureValidator(
            final RealmConfig realmConfig,
            final SSLService sslService,
            final PkcJwkSetReloadNotifier reloadNotifier
        ) {
            this.realmConfig = realmConfig;

            // Split configured signature algorithms by PKC and HMAC. Useful during validation, error logging, and JWK vs Alg filtering.
            final List<String> algs = realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            this.allowedJwksAlgsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
            this.allowedJwksAlgsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();

            // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
            final String jwkSetPath = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
            final SecureString hmacJwkSetContents = realmConfig.getSetting(JwtRealmSettings.HMAC_JWKSET);
            final SecureString hmacKeyContents = realmConfig.getSetting(JwtRealmSettings.HMAC_KEY);
            final boolean isConfiguredJwkSetPkc = Strings.hasText(jwkSetPath);
            final boolean isConfiguredJwkSetHmac = Strings.hasText(hmacJwkSetContents);
            final boolean isConfiguredJwkOidcHmac = Strings.hasText(hmacKeyContents);
            if (isConfiguredJwkSetPkc == false && isConfiguredJwkSetHmac == false && isConfiguredJwkOidcHmac == false) {
                throw new SettingsException(
                    "At least one of ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY)
                        + "] or ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET)
                        + "] or ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH)
                        + "] must be set"
                );
            }
            if (isConfiguredJwkSetHmac && isConfiguredJwkOidcHmac) {
                // HMAC Key vs HMAC JWKSet settings must be mutually exclusive
                throw new SettingsException(
                    "Settings ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET)
                        + "] and ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY)
                        + "] are not allowed at the same time."
                );
            }

            final List<JWK> jwksHmac;
            if (isConfiguredJwkSetHmac) {
                jwksHmac = JwkValidateUtil.loadJwksFromJwkSetString(
                    RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET),
                    hmacJwkSetContents.toString()
                );
            } else if (isConfiguredJwkOidcHmac) {
                final OctetSequenceKey hmacKey = JwkValidateUtil.loadHmacJwkFromJwkString(
                    RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY),
                    hmacKeyContents
                );
                assert hmacKey != null : "Null HMAC key should not happen here";
                jwksHmac = List.of(hmacKey);
            } else {
                jwksHmac = null;
            }

            if (jwksHmac != null) {
                final JwkSetLoader.JwksAlgs jwksAlgs = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, allowedJwksAlgsHmac);
                logger.info("Usable HMAC: JWKs [{}]. Algorithms [{}].", jwksAlgs.jwks().size(), String.join(",", jwksAlgs.algs()));
                // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
                this.hmacJwtSignatureValidator = new HmacJwtSignatureValidator(jwksAlgs);
            } else {
                this.hmacJwtSignatureValidator = FAILING_JWT_SIGNATURE_VALIDATOR;
            }

            if (isConfiguredJwkSetPkc) {
                this.pkcJwtSignatureValidator = new PkcJwtSignatureValidator(new JwkSetLoader(realmConfig, sslService), reloadNotifier);
            } else {
                this.pkcJwtSignatureValidator = FAILING_JWT_SIGNATURE_VALIDATOR;
            }

            if (false == hasUsableJwksAlgs()) {
                throw new SettingsException(
                    "No available JWK and algorithm for HMAC or PKC. Realm authentication expected to fail until this is fixed."
                );

            }
        }

        @Override
        public void validate(String tokenPrincipal, SignedJWT jwt, ActionListener<Void> listener) {
            final String algorithm = jwt.getHeader().getAlgorithm().getName();

            if (allowedJwksAlgsHmac.contains(algorithm)) {
                hmacJwtSignatureValidator.validate(tokenPrincipal, jwt, listener);
            } else if (allowedJwksAlgsPkc.contains(algorithm)) {
                pkcJwtSignatureValidator.validate(tokenPrincipal, jwt, listener);
            } else {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "algorithm [%s] is not in the list of supported algorithms [%s]",
                        algorithm,
                        Strings.collectionToCommaDelimitedString(
                            Stream.of(allowedJwksAlgsHmac.stream(), allowedJwksAlgsPkc.stream()).toList()
                        )
                    )
                );
            }
        }

        @Override
        public void close() {
            pkcJwtSignatureValidator.close();
        }

        @Override
        public boolean hasUsableJwksAlgs() {
            return hmacJwtSignatureValidator.hasUsableJwksAlgs() || pkcJwtSignatureValidator.hasUsableJwksAlgs();
        }

        // Package private for testing only
        Tuple<JwkSetLoader.JwksAlgs, JwkSetLoader.JwksAlgs> getAllJwksAlgs() {
            final JwkSetLoader.JwksAlgs jwksAlgsHmac;
            if (hmacJwtSignatureValidator == FAILING_JWT_SIGNATURE_VALIDATOR) {
                jwksAlgsHmac = new JwkSetLoader.JwksAlgs(List.of(), List.of());
            } else {
                jwksAlgsHmac = ((HmacJwtSignatureValidator) hmacJwtSignatureValidator).jwksAlgs;
            }

            final JwkSetLoader.JwksAlgs jwksAlgsPkc;
            if (pkcJwtSignatureValidator == FAILING_JWT_SIGNATURE_VALIDATOR) {
                jwksAlgsPkc = new JwkSetLoader.JwksAlgs(List.of(), List.of());
            } else {
                jwksAlgsPkc = ((PkcJwtSignatureValidator) pkcJwtSignatureValidator).jwkSetLoader.getContentAndJwksAlgs().jwksAlgs();
            }
            return new Tuple<>(jwksAlgsHmac, jwksAlgsPkc);
        }
    }

    class HmacJwtSignatureValidator implements JwtSignatureValidator {

        private final JwkSetLoader.JwksAlgs jwksAlgs;

        HmacJwtSignatureValidator(JwkSetLoader.JwksAlgs jwksAlgs) {
            this.jwksAlgs = jwksAlgs;
        }

        public void validate(String tokenPrincipal, SignedJWT jwt, ActionListener<Void> listener) {
            // TODO: assert algorithm?
            try {
                JwtValidateUtil.validateSignature(jwt, jwksAlgs.jwks());
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(new ElasticsearchSecurityException("signature validation failed", e));
            }
        }

        @Override
        public boolean hasUsableJwksAlgs() {
            return false == jwksAlgs.isEmpty();
        }
    }

    class PkcJwtSignatureValidator implements JwtSignatureValidator {

        private static final Logger logger = LogManager.getLogger(PkcJwtSignatureValidator.class);

        private final JwkSetLoader jwkSetLoader;
        private final PkcJwkSetReloadNotifier reloadNotifier;

        PkcJwtSignatureValidator(JwkSetLoader jwkSetLoader, PkcJwkSetReloadNotifier reloadNotifier) {
            this.jwkSetLoader = jwkSetLoader;
            this.reloadNotifier = reloadNotifier;
        }

        public void validate(String tokenPrincipal, SignedJWT signedJWT, ActionListener<Void> listener) {
            // TODO: assert algorithm?
            try {
                JwtValidateUtil.validateSignature(signedJWT, jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().jwks());
                listener.onResponse(null);
            } catch (Exception primaryException) {
                logger.debug(
                    () -> org.elasticsearch.core.Strings.format(
                        "Signature verification failed for JWT [%s] reloading JWKSet (was: #[%s] JWKs, #[%s] algs, sha256=[%s])",
                        tokenPrincipal,
                        jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().jwks().size(),
                        jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().algs().size(),
                        MessageDigests.toHexString(jwkSetLoader.getContentAndJwksAlgs().sha256())
                    ),
                    primaryException
                );

                jwkSetLoader.reload(ActionListener.wrap(isUpdated -> {
                    if (false == isUpdated) {
                        // No change in JWKSet
                        logger.debug("Reloaded same PKC JWKs, can't retry verify JWT token [{}]", tokenPrincipal);
                        listener.onFailure(primaryException);
                        return;
                    }
                    // If all PKC JWKs were replaced, all PKC JWT cache entries need to be invalidated.
                    // Enhancement idea: Use separate caches for PKC vs HMAC JWKs, so only PKC entries get invalidated.
                    // Enhancement idea: When some JWKs are retained (ex: rotation), only invalidate for removed JWKs.
                    reloadNotifier.reloaded();

                    if (jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().isEmpty()) {
                        logger.debug("Reloaded empty PKC JWKs, signature verification will fail for JWT [{}]", tokenPrincipal);
                        // fall through and let try/catch below handle empty JWKs failure log and response
                    }

                    try {
                        JwtValidateUtil.validateSignature(signedJWT, jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().jwks());
                        listener.onResponse(null);
                    } catch (Exception secondaryException) {
                        logger.debug(
                            "Signature verification of JWT [{}] failed - original failure: [{}], failure after reload: [{}]",
                            tokenPrincipal,
                            primaryException.getMessage(),
                            secondaryException.getMessage()
                        );
                        secondaryException.addSuppressed(primaryException);
                        listener.onFailure(secondaryException);
                    }
                }, listener::onFailure));
            }
        }

        @Override
        public void close() {
            jwkSetLoader.close();
        }

        @Override
        public boolean hasUsableJwksAlgs() {
            return false == jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().isEmpty();
        }
    }

    interface PkcJwkSetReloadNotifier {
        void reloaded();
    }
}
