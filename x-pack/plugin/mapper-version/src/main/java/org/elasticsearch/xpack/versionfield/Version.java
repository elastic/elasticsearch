/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.BytesRefProducer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Script value class.
 * TODO(stu): implement {@code Comparable<Version>} based on {@code VersionEncoder#prefixDigitGroupsWithLength(String, BytesRefBuilder)}
 * See: https://github.com/elastic/elasticsearch/issues/82287
 */
public class Version implements ToXContent, BytesRefProducer, Comparable<Version> {
    protected String version;

    public Version(String version) {
        this.version = version;
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
    public boolean isFragment() {
        return false;
    }

    @Override
    public BytesRef toBytesRef() {
        //TODO cache it, or even better, copy it when the object is created
        return VersionEncoder.encodeVersion(version).bytesRef;
    }

    @Override
    public int compareTo(Version o) {
        return toBytesRef().compareTo(o.toBytesRef());
    }
}
