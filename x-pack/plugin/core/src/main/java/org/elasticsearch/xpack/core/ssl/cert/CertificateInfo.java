/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl.cert;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * Simple model of an X.509 certificate that is known to X-Pack
 */
public class CertificateInfo implements ToXContentObject, Writeable {
    private final String path;
    private final String format;
    private final String alias;
    private final String subjectDn;
    private final String serialNumber;
    private final boolean hasPrivateKey;
    private final ZonedDateTime expiry;

    public CertificateInfo(String path, String format, String alias, boolean hasPrivateKey, X509Certificate certificate) {
        Objects.requireNonNull(certificate, "Certificate cannot be null");
        this.path = Objects.requireNonNull(path, "Certificate path cannot be null");
        this.format = Objects.requireNonNull(format, "Certificate format cannot be null");
        this.alias = alias;
        this.subjectDn = Objects.requireNonNull(certificate.getSubjectDN().getName());
        this.serialNumber = certificate.getSerialNumber().toString(16);
        this.hasPrivateKey = hasPrivateKey;
        this.expiry = certificate.getNotAfter().toInstant().atZone(ZoneOffset.UTC);
    }

    public CertificateInfo(StreamInput in) throws IOException {
        this.path = in.readString();
        this.format = in.readString();
        this.alias = in.readOptionalString();
        this.subjectDn = in.readString();
        this.serialNumber = in.readString();
        this.hasPrivateKey = in.readBoolean();
        this.expiry = Instant.ofEpochMilli(in.readLong()).atZone(ZoneOffset.UTC);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeString(format);
        out.writeOptionalString(alias);
        out.writeString(subjectDn);
        out.writeString(serialNumber);
        out.writeBoolean(hasPrivateKey);
        out.writeLong(expiry.toInstant().toEpochMilli());
    }

    public String path() {
        return path;
    }

    public String format() {
        return format;
    }

    public String alias() {
        return alias;
    }

    public String subjectDn() {
        return subjectDn;
    }

    public String serialNumber() {
        return serialNumber;
    }

    public ZonedDateTime expiry() {
        return expiry;
    }

    public boolean hasPrivateKey() {
        return hasPrivateKey;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("path", path)
                .field("format", format)
                .field("alias", alias)
                .field("subject_dn", subjectDn)
                .field("serial_number", serialNumber)
                .field("has_private_key", hasPrivateKey)
                .timeField("expiry", expiry)
                .endObject();
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
                && Objects.equals(this.serialNumber, that.serialNumber)
                && Objects.equals(this.subjectDn, that.subjectDn)
                && Objects.equals(this.expiry, that.expiry);
    }

    @Override
    public int hashCode() {
        int result = path.hashCode();
        result = 31 * result + (alias != null ? alias.hashCode() : 0);
        result = 31 * result + (serialNumber != null ? serialNumber.hashCode() : 0);
        return result;
    }
}
