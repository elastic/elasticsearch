/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * Simple wrapper around {@link ZoneId} so that it can be written to XContent
 */
public class WriteableZoneId implements Writeable, ToXContentFragment {

    private final ZoneId zoneId;

    public WriteableZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public WriteableZoneId(StreamInput in) throws IOException {
        zoneId = ZoneId.of(in.readString());
    }

    public static WriteableZoneId of(String input) {
        return new WriteableZoneId(ZoneId.of(input));
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(zoneId.getId());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(zoneId.getId());
    }

    @Override
    public String toString() {
        return zoneId.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WriteableZoneId that = (WriteableZoneId) o;
        return Objects.equals(zoneId, that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId);
    }
}
