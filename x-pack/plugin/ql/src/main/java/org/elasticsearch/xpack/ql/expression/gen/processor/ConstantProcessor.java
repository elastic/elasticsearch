/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class ConstantProcessor implements Processor {

    public static String NAME = "c";

    private Object constant;
    private final Type type;

    enum Type {
        NAMED_WRITABLE,
        ZONEDDATETIME,
        GENERIC,
        VERSION // Version is in x-pack, so StreamInput/Output cannot manage it as a generic type
    }

    public ConstantProcessor(Object value) {
        this.constant = value;
        if (value instanceof NamedWriteable) {
            type = Type.NAMED_WRITABLE;
        } else if (value instanceof ZonedDateTime) {
            type = Type.ZONEDDATETIME;
        } else if (value instanceof Version) {
            type = Type.VERSION;
        } else {
            type = Type.GENERIC;
        }
    }

    public ConstantProcessor(StreamInput in) throws IOException {
        type = in.readEnum(Type.class);
        switch (type) {
            case NAMED_WRITABLE -> constant = in.readNamedWriteable(ConstantNamedWriteable.class);
            case ZONEDDATETIME -> {
                ZonedDateTime zdt;
                ZoneId zoneId = in.readZoneId();
                zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readLong()), zoneId);
                constant = zdt.withNano(in.readInt());
            }
            case VERSION -> constant = new Version(in.readString());
            case GENERIC -> constant = in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
        switch (type) {
            case NAMED_WRITABLE -> out.writeNamedWriteable((NamedWriteable) constant);
            case ZONEDDATETIME -> {
                ZonedDateTime zdt = (ZonedDateTime) constant;
                out.writeZoneId(zdt.getZone());
                out.writeLong(zdt.toInstant().toEpochMilli());
                out.writeInt(zdt.getNano());
            }
            case VERSION -> out.writeString(constant.toString());
            case GENERIC -> out.writeGenericValue(constant);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return constant;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(constant);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConstantProcessor other = (ConstantProcessor) obj;
        return Objects.equals(constant, other.constant);
    }

    @Override
    public String toString() {
        return "^" + constant;
    }
}
