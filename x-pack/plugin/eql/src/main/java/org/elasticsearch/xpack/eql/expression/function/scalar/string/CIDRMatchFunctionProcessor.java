/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.util.Check;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CIDRMatchFunctionProcessor implements Processor {

    public static final String NAME = "cdrm";

    private final Processor source;
    private final List<Processor> addresses;

    public CIDRMatchFunctionProcessor(Processor source, List<Processor> addresses) {
        this.source = source;
        this.addresses = addresses;
    }

    public CIDRMatchFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        addresses = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteableList(addresses);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        Object src = source.process(input);
        ArrayList<Object> arr = new ArrayList<>(addresses.size());
        for (Processor address : addresses) {
            arr.add(address.process(input));
        }
        return doProcess(src, arr);
    }

    public static Object doProcess(Object source, List<Object> addresses) {
        if (source == null) {
            return null;
        }

        Check.isString(source);

        String[] arr = new String[addresses.size()];
        int i = 0;
        for (Object address : addresses) {
            Check.isString(address);
            arr[i++] = (String) address;
        }
        try {
            return CIDRUtils.isInRange((String) source, arr);
        } catch (IllegalArgumentException e) {
            throw new EqlIllegalArgumentException(e.getMessage());
        }
    }

    protected Processor source() {
        return source;
    }

    public List<Processor> addresses() {
        return addresses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), addresses());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CIDRMatchFunctionProcessor other = (CIDRMatchFunctionProcessor) obj;
        return Objects.equals(source(), other.source()) && Objects.equals(addresses(), other.addresses());
    }
}
