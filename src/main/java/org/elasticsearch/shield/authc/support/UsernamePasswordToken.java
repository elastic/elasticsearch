/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class UsernamePasswordToken implements AuthenticationToken {

    public static final String BASIC_AUTH_HEADER = "Authorization";
    private static final Pattern BASIC_AUTH_PATTERN = Pattern.compile("Basic\\s(.+)");

    private final String username;
    private final char[] password;

    public UsernamePasswordToken(String username, char[] password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public String principal() {
        return username;
    }

    @Override
    public char[] credentials() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UsernamePasswordToken that = (UsernamePasswordToken) o;

        return Arrays.equals(password, that.password) &&
                Objects.equals(username, that.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, Arrays.hashCode(password));
    }

    public static UsernamePasswordToken extractToken(TransportMessage<?> message, UsernamePasswordToken defaultToken) {
        String authStr = message.getHeader(BASIC_AUTH_HEADER);
        if (authStr == null) {
            return defaultToken;
        }

        Matcher matcher = BASIC_AUTH_PATTERN.matcher(authStr.trim());
        if (!matcher.matches()) {
            throw new AuthenticationException("Invalid basic authentication header value");
        }

        String userpasswd = new String(Base64.decodeBase64(matcher.group(1)), Charsets.UTF_8);
        int i = userpasswd.indexOf(':');
        if (i < 0) {
            throw new AuthenticationException("Invalid basic authentication header value");
        }
        return new UsernamePasswordToken(userpasswd.substring(0, i), userpasswd.substring(i+1).toCharArray());
    }

    public static UsernamePasswordToken extractToken(RestRequest request, UsernamePasswordToken defaultToken) {
        String authStr = request.header(BASIC_AUTH_HEADER);
        if (authStr == null) {
            return defaultToken;
        }

        Matcher matcher = BASIC_AUTH_PATTERN.matcher(authStr.trim());
        if (!matcher.matches()) {
            throw new AuthenticationException("Invalid basic authentication header value");
        }

        String userpasswd = new String(Base64.decodeBase64(matcher.group(1)), Charsets.UTF_8);
        int i = userpasswd.indexOf(':');
        if (i < 0) {
            throw new AuthenticationException("Invalid basic authentication header value");
        }
        return new UsernamePasswordToken(userpasswd.substring(0, i), userpasswd.substring(i+1).toCharArray());
    }

    public static void putTokenHeader(TransportRequest request, UsernamePasswordToken token) {
        request.putHeader("Authorization", basicAuthHeaderValue(token.username, token.password));
    }

    public static String basicAuthHeaderValue(String username, char[] passwd) {
        String basicToken = username + ":" + new String(passwd);
        basicToken = new String(Base64.encodeBase64(basicToken.getBytes(Charsets.UTF_8)), Charsets.UTF_8);
        return "Basic " + basicToken;
    }
}
