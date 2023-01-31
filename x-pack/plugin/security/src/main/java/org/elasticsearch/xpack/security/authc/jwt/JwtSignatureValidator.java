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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
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

    class DelegatingJwtSignatureValidator implements JwtSignatureValidator {

        private static final Logger logger = LogManager.getLogger(DelegatingJwtSignatureValidator.class);

        private final RealmConfig realmConfig;
        final List<String> allowedJwksAlgsPkc;
        final List<String> allowedJwksAlgsHmac;
        @Nullable
        private final HmacJwtSignatureValidator hmacJwtSignatureValidator;
        @Nullable
        private final PkcJwtSignatureValidator pkcJwtSignatureValidator;

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

            final String jwkSetPath = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
            final SecureString hmacJwkSetContents = realmConfig.getSetting(JwtRealmSettings.HMAC_JWKSET);
            final SecureString hmacKeyContents = realmConfig.getSetting(JwtRealmSettings.HMAC_KEY);
            final boolean isConfiguredJwkSetPkc = Strings.hasText(jwkSetPath);
            final boolean isConfiguredJwkSetHmac = Strings.hasText(hmacJwkSetContents);
            final boolean isConfiguredJwkOidcHmac = Strings.hasText(hmacKeyContents);
            validateJwkSettings(realmConfig, isConfiguredJwkSetPkc, isConfiguredJwkSetHmac, isConfiguredJwkOidcHmac);

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
                // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algorithms with a matching JWK.
                this.hmacJwtSignatureValidator = new HmacJwtSignatureValidator(jwksAlgs);
            } else {
                this.hmacJwtSignatureValidator = null;
            }

            if (isConfiguredJwkSetPkc) {
                this.pkcJwtSignatureValidator = new PkcJwtSignatureValidator(
                    new JwkSetLoader(realmConfig, allowedJwksAlgsPkc, sslService),
                    reloadNotifier
                );
            } else {
                this.pkcJwtSignatureValidator = null;
            }
            logWarnIfAuthenticationWillAlwaysFail();
        }

        @Override
        public void validate(String tokenPrincipal, SignedJWT jwt, ActionListener<Void> listener) {
            final String algorithm = jwt.getHeader().getAlgorithm().getName();
            if (allowedJwksAlgsHmac.contains(algorithm)) {
                if (hmacJwtSignatureValidator != null) {
                    hmacJwtSignatureValidator.validate(tokenPrincipal, jwt, listener);
                } else {
                    listener.onFailure(
                        new ElasticsearchSecurityException(
                            "algorithm [%s] is a HMAC signing algorithm, but none of the HMAC JWK settings ["
                                + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY)
                                + ", "
                                + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET)
                                + "] is configured",
                            RestStatus.BAD_REQUEST,
                            algorithm
                        )
                    );
                }
            } else if (allowedJwksAlgsPkc.contains(algorithm)) {
                if (pkcJwtSignatureValidator != null) {
                    pkcJwtSignatureValidator.validate(tokenPrincipal, jwt, listener);
                } else {
                    listener.onFailure(
                        new ElasticsearchSecurityException(
                            "algorithm [%s] is a PKC signing algorithm, but PKC JWK setting ["
                                + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH)
                                + "] is not configured",
                            RestStatus.BAD_REQUEST,
                            algorithm
                        )
                    );
                }
            } else {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "algorithm [%s] is not in the list of supported algorithms [%s]",
                        RestStatus.BAD_REQUEST,
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
            if (pkcJwtSignatureValidator != null) {
                pkcJwtSignatureValidator.close();
            }
        }

        private void logWarnIfAuthenticationWillAlwaysFail() {
            final boolean hasUsableJwksAndAlgorithms = (hmacJwtSignatureValidator != null
                && false == hmacJwtSignatureValidator.jwksAlgs.isEmpty())
                || (pkcJwtSignatureValidator != null
                    && false == pkcJwtSignatureValidator.jwkSetLoader.getContentAndJwksAlgs().jwksAlgs().isEmpty());
            if (false == hasUsableJwksAndAlgorithms) {
                logger.warn(
                    "No available JWK and algorithm for HMAC or PKC. JWT realm authentication expected to fail until this is fixed."
                );
            }
        }

        private static void validateJwkSettings(
            RealmConfig realmConfig,
            boolean isConfiguredJwkSetPkc,
            boolean isConfiguredJwkSetHmac,
            boolean isConfiguredJwkOidcHmac
        ) {
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

            // HMAC Key vs HMAC JWKSet settings must be mutually exclusive
            if (isConfiguredJwkSetHmac && isConfiguredJwkOidcHmac) {
                throw new SettingsException(
                    "Settings ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET)
                        + "] and ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY)
                        + "] are not allowed at the same time."
                );
            }
        }

        // Package private for testing Only
        Tuple<JwkSetLoader.JwksAlgs, JwkSetLoader.JwksAlgs> getAllJwksAlgs() {
            final JwkSetLoader.JwksAlgs jwksAlgsHmac;
            if (hmacJwtSignatureValidator == null) {
                jwksAlgsHmac = new JwkSetLoader.JwksAlgs(List.of(), List.of());
            } else {
                jwksAlgsHmac = hmacJwtSignatureValidator.jwksAlgs;
            }

            final JwkSetLoader.JwksAlgs jwksAlgsPkc;
            if (pkcJwtSignatureValidator == null) {
                jwksAlgsPkc = new JwkSetLoader.JwksAlgs(List.of(), List.of());
            } else {
                jwksAlgsPkc = pkcJwtSignatureValidator.jwkSetLoader.getContentAndJwksAlgs().jwksAlgs();
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
                listener.onFailure(e);
            }
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
            final JwkSetLoader.ContentAndJwksAlgs contentAndJwksAlgs = jwkSetLoader.getContentAndJwksAlgs();
            final JwkSetLoader.JwksAlgs jwksAlgs = contentAndJwksAlgs.jwksAlgs();
            try {
                JwtValidateUtil.validateSignature(signedJWT, jwksAlgs.jwks());
                listener.onResponse(null);
            } catch (Exception primaryException) {
                logger.debug(
                    () -> org.elasticsearch.core.Strings.format(
                        "Signature verification failed for JWT [%s] reloading JWKSet (was: #[%s] JWKs, #[%s] algs, sha256=[%s])",
                        tokenPrincipal,
                        jwksAlgs.jwks().size(),
                        jwksAlgs.algs().size(),
                        MessageDigests.toHexString(contentAndJwksAlgs.sha256())
                    ),
                    primaryException
                );

                jwkSetLoader.reload(ActionListener.wrap(reloadResult -> {
                    if (false == reloadResult.v1()) {
                        // No change in JWKSet
                        logger.debug("Reloaded same PKC JWKs, can't retry verify JWT token [{}]", tokenPrincipal);
                        listener.onFailure(primaryException);
                        return;
                    }
                    // If all PKC JWKs were replaced, all PKC JWT cache entries need to be invalidated.
                    // Enhancement idea: Use separate caches for PKC vs HMAC JWKs, so only PKC entries get invalidated.
                    // Enhancement idea: When some JWKs are retained (ex: rotation), only invalidate for removed JWKs.
                    reloadNotifier.reloaded();

                    final JwkSetLoader.JwksAlgs reloadedJwksAlgs = reloadResult.v2();
                    if (reloadedJwksAlgs.isEmpty()) {
                        logger.debug("Reloaded empty PKC JWKs, signature verification will fail for JWT [{}]", tokenPrincipal);
                        // fall through and let try/catch below handle empty JWKs failure log and response
                    }

                    try {
                        JwtValidateUtil.validateSignature(signedJWT, reloadedJwksAlgs.jwks());
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
    }

    interface PkcJwkSetReloadNotifier {
        void reloaded();
    }
}
