/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Representation of signature algorithms for JWT as described in <a href="https://tools.ietf.org/html/rfc7518#section-3.1">RFC7518</a>
 */
public enum SignatureAlgorithm {

    NONE("NONE", "NONE"),
    HS256("HS256", "HmacSHA256"),
    HS384("HS384", "HmacSHA384"),
    HS512("HS512", "HmacSHA512"),
    RS256("RS256", "SHA256WithRSA"),
    RS384("RS384", "SHA384WithRSA"),
    RS512("RS512", "SHA512WithRSA"),
    ES256("ES256", "SHA256WithECDSA"),
    ES384("ES384", "SHA384WithECDSA"),
    ES512("ES512", "SHA512WithECDSA");

    private String name;
    private String jcaAlgoName;

    SignatureAlgorithm(String name, String jcaAlgoName) {
        this.name = name;
        this.jcaAlgoName = jcaAlgoName;
    }

    public static SignatureAlgorithm fromName(String alg) {
        return Stream.of(SignatureAlgorithm.values()).filter(n -> n.name().equals(alg)).findFirst().orElse(null);
    }

    public String getJcaAlgoName() {
        return jcaAlgoName;
    }

    public static List<SignatureAlgorithm> getHmacAlgorithms() {
        return Arrays.asList(HS256, HS384, HS512);
    }

    public static List<SignatureAlgorithm> getRsaAlgorithms() {
        return Arrays.asList(RS256, RS384, RS512);
    }

    public static List<SignatureAlgorithm> getEcAlgorithms() {
        return Arrays.asList(ES256, ES384, ES512);
    }

    public static List<String> getAllNames() {
        return Stream.of(SignatureAlgorithm.values()).map(Enum::name).collect(Collectors.toList());
    }
}
