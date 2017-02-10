/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.User;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

public class Authentication {

    public static final String AUTHENTICATION_KEY = "_xpack_security_authentication";

    private final User user;
    private final RealmRef authenticatedBy;
    private final RealmRef lookedUpBy;
    private final Version version;

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy) {
        this.user = Objects.requireNonNull(user);
        this.authenticatedBy = Objects.requireNonNull(authenticatedBy);
        this.lookedUpBy = lookedUpBy;
        this.version = Version.CURRENT;
    }

    public Authentication(StreamInput in) throws IOException {
        this.user = User.readFrom(in);
        this.authenticatedBy = new RealmRef(in);
        if (in.readBoolean()) {
            this.lookedUpBy = new RealmRef(in);
        } else {
            this.lookedUpBy = null;
        }
        this.version = in.getVersion();
    }

    public User getUser() {
        return user;
    }

    // TODO remove run as from the User object...
    public User getRunAsUser() {
        if (user.runAs() != null) {
            return user.runAs();
        }
        return user;
    }

    /**
     * returns true if this authentication represents a authentication object with a authenticated user that is different than the user the
     * request should be run as
     */
    public boolean isRunAs() {
        return getUser().equals(getRunAsUser()) == false;
    }

    public RealmRef getAuthenticatedBy() {
        return authenticatedBy;
    }

    public RealmRef getLookedUpBy() {
        return lookedUpBy;
    }

    public Version getVersion() {
        return version;
    }

    public static Authentication readFromContext(ThreadContext ctx, CryptoService cryptoService, boolean sign)
            throws IOException, IllegalArgumentException {
        Authentication authentication = ctx.getTransient(AUTHENTICATION_KEY);
        if (authentication != null) {
            assert ctx.getHeader(AUTHENTICATION_KEY) != null;
            return authentication;
        }

        String authenticationHeader = ctx.getHeader(AUTHENTICATION_KEY);
        if (authenticationHeader == null) {
            return null;
        }
        return deserializeHeaderAndPutInContext(authenticationHeader, ctx, cryptoService, sign);
    }

    public static Authentication getAuthentication(ThreadContext context) {
        return context.getTransient(Authentication.AUTHENTICATION_KEY);
    }

    static Authentication deserializeHeaderAndPutInContext(String header, ThreadContext ctx, CryptoService cryptoService, boolean sign)
            throws IOException, IllegalArgumentException {
        assert ctx.getTransient(AUTHENTICATION_KEY) == null;
        if (sign) {
            header = cryptoService.unsignAndVerify(header);
        }

        byte[] bytes = Base64.getDecoder().decode(header);
        StreamInput input = StreamInput.wrap(bytes);
        Version version = Version.readVersion(input);
        input.setVersion(version);
        Authentication authentication = new Authentication(input);
        ctx.putTransient(AUTHENTICATION_KEY, authentication);
        return authentication;
    }

    void writeToContextIfMissing(ThreadContext context, CryptoService cryptoService, boolean sign)
            throws IOException, IllegalArgumentException {
        if (context.getTransient(AUTHENTICATION_KEY) != null) {
            if (context.getHeader(AUTHENTICATION_KEY) == null) {
                throw new IllegalStateException("authentication present as a transient but not a header");
            }
            return;
        }

        if (context.getHeader(AUTHENTICATION_KEY) != null) {
            deserializeHeaderAndPutInContext(context.getHeader(AUTHENTICATION_KEY), context, cryptoService, sign);
        } else {
            writeToContext(context, cryptoService, sign);
        }
    }

    /**
     * Writes the authentication to the context. There must not be an existing authentication in the context and if there is an
     * {@link IllegalStateException} will be thrown
     */
    public void writeToContext(ThreadContext ctx, CryptoService cryptoService, boolean sign)
            throws IOException, IllegalArgumentException {
        ensureContextDoesNotContainAuthentication(ctx);
        String header = encode();
        if (sign) {
            header = cryptoService.sign(header);
        }
        ctx.putTransient(AUTHENTICATION_KEY, this);
        ctx.putHeader(AUTHENTICATION_KEY, header);
    }

    void ensureContextDoesNotContainAuthentication(ThreadContext ctx) {
        if (ctx.getTransient(AUTHENTICATION_KEY) != null) {
            if (ctx.getHeader(AUTHENTICATION_KEY) == null) {
                throw new IllegalStateException("authentication present as a transient but not a header");
            }
            throw new IllegalStateException("authentication is already present in the context");
        }
    }

    String encode() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        Version.writeVersion(Version.CURRENT, output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }

    void writeTo(StreamOutput out) throws IOException {
        User.writeTo(user, out);
        authenticatedBy.writeTo(out);
        if (lookedUpBy != null) {
            out.writeBoolean(true);
            lookedUpBy.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public static class RealmRef {

        private final String nodeName;
        private final String name;
        private final String type;

        public RealmRef(String name, String type, String nodeName) {
            this.nodeName = nodeName;
            this.name = name;
            this.type = type;
        }

        public RealmRef(StreamInput in) throws IOException {
            this.nodeName = in.readString();
            this.name = in.readString();
            this.type = in.readString();
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeName);
            out.writeString(name);
            out.writeString(type);
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }
}

