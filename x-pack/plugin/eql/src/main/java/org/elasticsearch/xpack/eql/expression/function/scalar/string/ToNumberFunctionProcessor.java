/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

public class ToNumberFunctionProcessor implements Processor {

    public static final String NAME = "num";

    private final Processor value, base;

    public ToNumberFunctionProcessor(Processor value, Processor base) {
        this.value = value;
        this.base = base;
    }

    public ToNumberFunctionProcessor(StreamInput in) throws IOException {
        value = in.readNamedWriteable(Processor.class);
        base = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(value);
    }

    @Override
    public Object process(Object input) {
        return doProcess(value.process(input), base.process(input));
    }

    public static Integer parseHexadecimal(String source) {
        source = source.startsWith("0x") ? source.substring(2) : source;
        return Integer.parseInt(source, 16);
    }

    public static Object doProcess(Object source, Object base) {
        if (source == null) {
            return null;
        }

        if (source instanceof Number) {
            return source;
        }

        if (!(source instanceof String || source instanceof Character)) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }

        boolean detectedHexPrefix = source.toString().startsWith("0x");

        if (base == null) {
            base = detectedHexPrefix ? 16 : 10;
        } else if (base instanceof Number == false) {
            throw new EqlIllegalArgumentException("An integer base is required; received [{}]", source);
        }

        int radix = ((Number) base).intValue();

        if (detectedHexPrefix && radix == 16) {
            source = source.toString().substring(2);
        }

        // custom bases need to use parseInt
        if (radix != 10) {
            return Integer.parseInt(source.toString(), radix);
        }

        // otherwise, we should allow for doubles
        try {
            return NumberFormat.getNumberInstance(Locale.US).parse(source.toString());
        } catch (ParseException e) {
            throw new EqlIllegalArgumentException("Unable to convert [{}] to number of base [{}]", source, radix);
        }

    }

    protected Processor source() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ToNumberFunctionProcessor other = (ToNumberFunctionProcessor) obj;
        return Objects.equals(source(), other.source());
    }

    @Override
    public int hashCode() {
        return Objects.hash(source());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
