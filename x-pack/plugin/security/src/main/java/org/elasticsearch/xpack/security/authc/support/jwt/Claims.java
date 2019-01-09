package org.elasticsearch.xpack.security.authc.support.jwt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Claims {

    public enum HeaderClaims {
        TYPE("type"),
        ALGORITHM("alg"),
        CONTENT_TYPE("cty"),
        KEY_ID("kid");

        private String name;

        HeaderClaims(String name) {
            this.name = name;
        }

        public String getClaimName() {
            return name;
        }

        public static List<String> validHeaderClaims() {
            return Stream.of(HeaderClaims.values()).map(HeaderClaims::getClaimName).collect(Collectors.toList());
        }

    }

    public enum StandardClaims {

        ISSUER("iss", "string"),
        SUBJECT("sub", "string"),
        AUDIENCE("aud", "array"),
        EXPIRATION_TIME("exp", "long"),
        NOT_BEFORE("nbf", "long"),
        ISSUED_AT("iat", "long"),
        NONCE("nonce", "string"),
        AUTHN_CONTEXT_CLASS_REF("acr", "array"),
        AUTHN_METHODS_REF("amr", "string"),
        AUTHORIZED_PARTY("azp", "string"),
        AUTH_TIME("auth_time", "long"),
        JWTID("jti", "string"),
        NAME("name", "string"),
        GIVEN_NAME("given_name", "string"),
        MIDDLE_NAME("middle_name", "string"),
        FAMILY_NAME("family_name", "string"),
        NICKNAME("nickname", "string"),
        PREFERRED_USERNAME("preferred_username", "string"),
        PROFILE("profile", "string"),
        PICTURE("picture", "string"),
        WEBSITE("website", "string"),
        EMAIL("email", "string"),
        EMAIL_VERIFIED("email_verified", "boolean"),
        GENDER("gender", "string"),
        BIRTHDATE("birthdate", "string"),
        ZONEINFO("zoneinfo", "string"),
        LOCALE("locale", "string"),
        PHONE_NUMBER("phone_number", "string"),
        PHONE_NUMBER_VERIFIED("phone_number_verified", "boolean"),
        ADDRESS("address", "object"),
        UPDATED_AT("updated_at", "long");

        private String name;
        private String type;

        StandardClaims(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getClaimName() {
            return name;
        }

        public static List<String> getStandardClaims() {
            return Stream.of(StandardClaims.values()).map(StandardClaims::getClaimName).collect(Collectors.toList());
        }

        public static List<String> getClaimsOfType(String type) {
            return Stream.of(StandardClaims.values())
                .filter(claim -> claim.getType().equals(type))
                .map(StandardClaims::getClaimName)
                .collect(Collectors.toList());
        }
    }
}
