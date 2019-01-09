/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Allows easier creation of {@link JsonWebToken}. Provides setters for the required and the standard claims
 * as defined in https://openid.net/specs/openid-connect-core-1_0.html
 */
public class JsonWebTokenBuilder {

    private String issuer;
    private String subject;
    private List<String> audiences;
    private long expirationTime = -1;
    private long notBefore = -1;
    private long issuedAt = -1;
    private long updatedAt = -1;
    private long authTime = -1;
    private String nonce;
    private String authenticationContextClassReference;
    private List<String> authenticationMethodsReferences;
    private String authorizedParty;
    private String jwtId;
    private String type;
    private String algorithm;
    private String name;
    private String givenName;
    private String middleName;
    private String familyName;
    private String nickname;
    private String preferredUsername;
    private String profile;
    private String picture;
    private String website;
    private String email;
    private Boolean emailVerified;
    private String gender;
    private String birthdate;
    private String zoneinfo;
    private String locale;
    private String phoneNumber;
    private Boolean phoneNumberVerified;
    private Map<String, Object> address;
    private Map<String, Object> claims;

    public JsonWebTokenBuilder name(String name) {
        this.name = name;
        return this;
    }

    public JsonWebTokenBuilder givenName(String givenName) {
        this.givenName = givenName;
        return this;
    }

    public JsonWebTokenBuilder middleName(String middleName) {
        this.middleName = middleName;
        return this;
    }

    public JsonWebTokenBuilder familyName(String familyName) {
        this.familyName = familyName;
        return this;
    }

    public JsonWebTokenBuilder nickname(String nickname) {
        this.nickname = nickname;
        return this;
    }

    public JsonWebTokenBuilder preferredUsername(String preferredUsername) {
        this.preferredUsername = preferredUsername;
        return this;
    }

    public JsonWebTokenBuilder profile(String profile) {
        this.profile = profile;
        return this;
    }

    public JsonWebTokenBuilder picture(String picture) {
        this.picture = picture;
        return this;
    }

    public JsonWebTokenBuilder website(String website) {
        this.website = website;
        return this;
    }

    public JsonWebTokenBuilder email(String email) {
        this.email = email;
        return this;
    }

    public JsonWebTokenBuilder emailVerified(boolean emailVerified) {
        this.emailVerified = emailVerified;
        return this;
    }

    public JsonWebTokenBuilder gender(String gender) {
        this.gender = gender;
        return this;
    }

    public JsonWebTokenBuilder birthdate(String birthdate) {
        this.birthdate = birthdate;
        return this;
    }

    public JsonWebTokenBuilder zoneinfo(String zoneinfo) {
        this.zoneinfo = zoneinfo;
        return this;
    }

    public JsonWebTokenBuilder locale(String locale) {
        this.locale = locale;
        return this;
    }

    public JsonWebTokenBuilder phoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        return this;
    }

    public JsonWebTokenBuilder phoneNumberVerified(boolean phoneNumberVerified) {
        this.phoneNumberVerified = phoneNumberVerified;
        return this;
    }

    public JsonWebTokenBuilder address(Map<String, Object> address) {
        this.address = address;
        return this;
    }

    public JsonWebTokenBuilder issuer(String issuer) {
        this.issuer = issuer;
        return this;
    }

    public JsonWebTokenBuilder subject(String subject) {
        this.subject = subject;
        return this;
    }

    public JsonWebTokenBuilder audience(String audience) {
        if (null == this.audiences) {
            this.audiences = new ArrayList<>();
        }
        this.audiences.add(audience);
        return this;
    }

    public JsonWebTokenBuilder jwtId(String jwtId) {
        this.jwtId = jwtId;
        return this;
    }

    public JsonWebTokenBuilder expirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public JsonWebTokenBuilder notBefore(long notBefore) {
        this.notBefore = notBefore;
        return this;
    }

    public JsonWebTokenBuilder issuedAt(long issuedAt) {
        this.issuedAt = issuedAt;
        return this;
    }

    public JsonWebTokenBuilder updatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
        return this;
    }

    public JsonWebTokenBuilder authTime(long authTime) {
        this.authTime = authTime;
        return this;
    }

    public JsonWebTokenBuilder nonce(String nonce) {
        this.nonce = nonce;
        return this;
    }

    public JsonWebTokenBuilder authenticationContectClassReference(String authenticationContextClassReference) {
        this.authenticationContextClassReference = authenticationContextClassReference;
        return this;
    }

    public JsonWebTokenBuilder authenticationMethodsReferences(String authenticationMethodsReferences) {
        if (this.authenticationMethodsReferences == null) {
            this.authenticationMethodsReferences = new ArrayList<>();
        }
        this.authenticationMethodsReferences.add(authenticationMethodsReferences);
        return this;
    }

    public JsonWebTokenBuilder authorizedParty(String authorizedParty) {
        this.authorizedParty = authorizedParty;
        return this;
    }

    public JsonWebTokenBuilder type(String type) {
        if (Strings.hasText(type) == false) {
            throw new IllegalArgumentException("JWT type cannot be null or empty");
        }
        this.type = type;
        return this;
    }

    public JsonWebTokenBuilder algorithm(String algorithm) {
        if (Strings.hasText(algorithm) == false) {
            throw new IllegalArgumentException("JWT signing algorithm cannot be null or empty");
        }
        this.algorithm = algorithm;
        return this;
    }

    public JsonWebTokenBuilder claim(String name, Object value) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("Claim name cannot be null or empty");
        }
        getOrCreateClaims().put(name, value);
        return this;
    }

    private Map<String, Object> getOrCreateClaims() {
        if (claims == null) {
            claims = new HashMap<>();
            return claims;
        } else {
            return claims;
        }
    }

    public JsonWebToken build() {
        final Map<String, Object> header = new HashMap<>();
        putIfNotNull(header, "typ", type);
        putIfNotNull(header, "alg", algorithm);

        final Map<String, Object> payload = new HashMap<>();
        putIfNotNull(payload, "iss", issuer);
        putIfNotNull(payload, "sub", subject);
        putIfNotNull(payload, "aud", audiences);
        if (expirationTime != -1) {
            putIfNotNull(payload, "exp", expirationTime);
        }
        if (notBefore != -1) {
            putIfNotNull(payload, "nbf", notBefore);
        }
        if (issuedAt != -1) {
            putIfNotNull(payload, "iat", issuedAt);
        }
        if (authTime != -1) {
            putIfNotNull(payload, "auth_time", authTime);
        }
        putIfNotNull(payload, "nonce", nonce);
        putIfNotNull(payload, "acr", authenticationContextClassReference);
        putIfNotNull(payload, "amr", authenticationMethodsReferences);
        putIfNotNull(payload, "azp", authorizedParty);
        putIfNotNull(payload, "jti", jwtId);
        putIfNotNull(payload, "name", name);
        putIfNotNull(payload, "given_name", givenName);
        putIfNotNull(payload, "middle_name", middleName);
        putIfNotNull(payload, "family_name", familyName);
        putIfNotNull(payload, "nickname", nickname);
        putIfNotNull(payload, "preferred_username", preferredUsername);
        putIfNotNull(payload, "profile", profile);
        putIfNotNull(payload, "picture", picture);
        putIfNotNull(payload, "website", website);
        putIfNotNull(payload, "email", email);
        putIfNotNull(payload, "email_verified", emailVerified);
        putIfNotNull(payload, "gender", gender);
        putIfNotNull(payload, "birthdate", birthdate);
        putIfNotNull(payload, "zoneinfo", zoneinfo);
        putIfNotNull(payload, "locale", locale);
        putIfNotNull(payload, "phone_number", phoneNumber);
        putIfNotNull(payload, "phone_number_verified", phoneNumberVerified);
        putIfNotNull(payload, "address", address);
        if (updatedAt != -1) {
            putIfNotNull(payload, "updated_at", updatedAt);
        }
        if (claims != null) {
            payload.putAll(claims);
        }
        return new JsonWebToken(header, payload);
    }

    /**
     * Adds a key - value pair to a Map, only if the value is not null.
     *
     * @param map   The Map to add the key value entry to
     * @param key   The key to populate
     * @param value The value to add to the respective key
     */
    private void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (null == map) {
            throw new IllegalArgumentException("The map must be provided");
        }
        if (key == null) {
            throw new IllegalArgumentException("The key must be provided");
        }
        if (value != null) {
            map.put(key, value);
        }
    }
}
