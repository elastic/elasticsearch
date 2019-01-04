package org.elasticsearch.xpack.security.authc.support.jwt;

import java.util.Arrays;
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

        ISSUER("iss"),
        SUBJECT("sub"),
        AUDIENCE("aud"),
        EXPIRATION_TIME("exp"),
        NOT_BEFORE("nbf"),
        ISSUED_AT("iat"),
        NONCE("nonce"),
        AUTHN_CONTEXT_CLASS_REF("acr"),
        AUTHN_METHODS_REF("amr"),
        AUTHORIZED_PARTY("azp"),
        AUTH_TIME("auth_time"),
        JWTID("jti"),
        NAME("name"),
        GIVEN_NAME("given_name"),
        MIDDLE_NAME("middle_name"),
        FAMILY_NAME("family_name"),
        NICKNAME("nickname"),
        PREFERRED_USERNAME("preferred_username"),
        PROFILE("profile"),
        PICTURE("picture"),
        WEBSITE("website"),
        EMAIL("email"),
        EMAIL_VERIFIED("email_verified"),
        GENDER("gender"),
        BIRTHDATE("birthdate"),
        ZONEINFO("zoneinfo"),
        LOCALE("locale"),
        PHONE_NUMBER("phone_number"),
        PHONE_NUMBER_VERIFIED("phone_number_verified"),
        ADDRESS("address"),
        UPDATED_AT("updated_at");

        private String name;

        StandardClaims(String name) {
            this.name = name;
        }

        public String getClaimName() {
            return name;
        }

        public static List<String> getKnownClaims() {
            return Stream.of(StandardClaims.values()).map(StandardClaims::getClaimName).collect(Collectors.toList());
        }
    }
}
