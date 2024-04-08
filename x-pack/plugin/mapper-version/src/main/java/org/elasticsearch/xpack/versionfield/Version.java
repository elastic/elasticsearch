/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.BytesRefProducer;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Version value class, also exposed to scripting consumers.
 */
public class Version implements ToXContentFragment, BytesRefProducer, Comparable<Version> {
    protected String version;
    protected BytesRef bytes;

    public Version(String version) {
        this.version = version;
        this.bytes = VersionEncoder.encodeVersion(version).bytesRef;
    }

    public Version(BytesRef bytes) {
        this.version = VersionEncoder.decodeVersion(bytes).utf8ToString();
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        return version;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }

    @Override
    public BytesRef toBytesRef() {
        return bytes;
    }

    @Override
    public int compareTo(Version o) {
        return toBytesRef().compareTo(o.toBytesRef());
    }
}
