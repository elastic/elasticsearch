/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;

public class UsernamePasswordToken implements AuthenticationToken {

    public static final String BASIC_AUTH_HEADER = "Authorization";
    private static final String BASIC_AUTH_PREFIX = "Basic ";

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

    public static UsernamePasswordToken extractToken(ThreadContext context) {
        String authStr = context.getHeader(BASIC_AUTH_HEADER);
        if (authStr == null) {
            return null;
        }

        return extractToken(authStr);
    }

    private static UsernamePasswordToken extractToken(String headerValue) {
        if (headerValue.startsWith(BASIC_AUTH_PREFIX) == false) {
            // the header does not start with 'Basic ' so we cannot use it, but it may be valid for another realm
            return null;
        }

        // if there is nothing after the prefix, the header is bad
        if (headerValue.length() == BASIC_AUTH_PREFIX.length()) {
            throw authenticationError("invalid basic authentication header value");
        }

        char[] userpasswd;
        try {
            userpasswd = CharArrays.utf8BytesToChars(Base64.getDecoder().decode(headerValue.substring(BASIC_AUTH_PREFIX.length()).trim()));
        } catch (IllegalArgumentException e) {
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

    public static void putTokenHeader(ThreadContext context, UsernamePasswordToken token) {
        context.putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue(token.username, token.password));
    }

    public static String basicAuthHeaderValue(String username, SecuredString passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        chars.put(username).put(':').put(passwd.internalChars());

        //TODO we still have passwords in Strings in headers
        String basicToken = Base64.getEncoder().encodeToString(CharArrays.toUtf8Bytes(chars.array()));
        return "Basic " + basicToken;
    }
}
