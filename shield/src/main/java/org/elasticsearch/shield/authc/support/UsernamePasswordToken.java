/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.Base64;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;

/**
 *
 */
public class UsernamePasswordToken implements AuthenticationToken {

    public static final String BASIC_AUTH_HEADER = "Authorization";
    private static final Pattern BASIC_AUTH_PATTERN = Pattern.compile("Basic\\s(.+)");

    private final String username;
    private final SecuredString password;

    public UsernamePasswordToken(String username, SecuredString password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public String principal() {
        return username;
    }

    @Override
    public SecuredString credentials() {
        return password;
    }

    @Override
    public void clearCredentials() {
        password.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UsernamePasswordToken that = (UsernamePasswordToken) o;

        return Objects.equals(password, that.password) &&
                Objects.equals(username, that.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, password.hashCode());
    }

    public static UsernamePasswordToken extractToken(TransportMessage<?> message, UsernamePasswordToken defaultToken) {
        String authStr = message.getHeader(BASIC_AUTH_HEADER);
        if (authStr == null) {
            return defaultToken;
        }

        return extractToken(authStr);
    }

    public static UsernamePasswordToken extractToken(RestRequest request, UsernamePasswordToken defaultToken) {
        String authStr = request.header(BASIC_AUTH_HEADER);
        if (authStr == null) {
            return defaultToken;
        }

        return extractToken(authStr);
    }

    static UsernamePasswordToken extractToken(String token) {
        Matcher matcher = BASIC_AUTH_PATTERN.matcher(token.trim());
        if (!matcher.matches()) {
            throw authenticationError("invalid basic authentication header value");
        }

        char[] userpasswd;
        try {
            userpasswd = CharArrays.utf8BytesToChars(Base64.decode(matcher.group(1)));
        } catch (IllegalArgumentException|IOException e) {
            throw authenticationError("invalid basic authentication header encoding", e);
        }

        int i = CharArrays.indexOf(userpasswd, ':');
        if (i < 0) {
            throw authenticationError("invalid basic authentication header value");
        }

        return new UsernamePasswordToken(
                new String(Arrays.copyOfRange(userpasswd, 0, i)),
                new SecuredString(Arrays.copyOfRange(userpasswd, i + 1, userpasswd.length)));
    }

    public static void putTokenHeader(TransportRequest request, UsernamePasswordToken token) {
        request.putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue(token.username, token.password));
    }

    public static String basicAuthHeaderValue(String username, SecuredString passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        chars.put(username).put(':').put(passwd.internalChars());

        //TODO we still have passwords in Strings in headers
        String basicToken = Base64.encodeBytes(CharArrays.toUtf8Bytes(chars.array()));
        return "Basic " + basicToken;
    }
}
