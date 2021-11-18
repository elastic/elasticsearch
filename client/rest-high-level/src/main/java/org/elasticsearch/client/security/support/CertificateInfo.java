/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Simple model of an X.509 certificate
 */
public final class CertificateInfo {
    public static final ParseField PATH = new ParseField("path");
    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField ALIAS = new ParseField("alias");
    public static final ParseField SUBJECT_DN = new ParseField("subject_dn");
    public static final ParseField SERIAL_NUMBER = new ParseField("serial_number");
    public static final ParseField HAS_PRIVATE_KEY = new ParseField("has_private_key");
    public static final ParseField EXPIRY = new ParseField("expiry");

    private final String path;
    private final String format;
    private final String alias;
    private final String subjectDn;
    private final String serialNumber;
    private final boolean hasPrivateKey;
    private final String expiry;

    public CertificateInfo(
        String path,
        String format,
        @Nullable String alias,
        String subjectDn,
        String serialNumber,
        boolean hasPrivateKey,
        String expiry
    ) {
        this.path = path;
        this.format = format;
        this.alias = alias;
        this.subjectDn = subjectDn;
        this.serialNumber = serialNumber;
        this.hasPrivateKey = hasPrivateKey;
        this.expiry = expiry;
    }

    public String getPath() {
        return path;
    }

    public String getFormat() {
        return format;
    }

    public String getAlias() {
        return alias;
    }

    public String getSubjectDn() {
        return subjectDn;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public boolean hasPrivateKey() {
        return hasPrivateKey;
    }

    public String getExpiry() {
        return expiry;
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<CertificateInfo, Void> PARSER = new ConstructingObjectParser<>(
        "certificate_info",
        true,
        args -> new CertificateInfo(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            (String) args[3],
            (String) args[4],
            (boolean) args[5],
            (String) args[6]
        )
    );

    static {
        PARSER.declareString(constructorArg(), PATH);
        PARSER.declareString(constructorArg(), FORMAT);
        PARSER.declareStringOrNull(constructorArg(), ALIAS);
        PARSER.declareString(constructorArg(), SUBJECT_DN);
        PARSER.declareString(constructorArg(), SERIAL_NUMBER);
        PARSER.declareBoolean(constructorArg(), HAS_PRIVATE_KEY);
        PARSER.declareString(constructorArg(), EXPIRY);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final CertificateInfo that = (CertificateInfo) other;
        return this.path.equals(that.path)
            && this.format.equals(that.format)
            && this.hasPrivateKey == that.hasPrivateKey
            && Objects.equals(this.alias, that.alias)
            && this.serialNumber.equals(that.serialNumber)
            && this.subjectDn.equals(that.subjectDn)
            && this.expiry.equals(that.expiry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, format, alias, subjectDn, serialNumber, hasPrivateKey, expiry);
    }

    public static CertificateInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
