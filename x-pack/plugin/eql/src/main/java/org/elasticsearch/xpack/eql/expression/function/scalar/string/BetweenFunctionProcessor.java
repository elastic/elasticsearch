/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.util.Check;

import java.io.IOException;
import java.util.Objects;

public class BetweenFunctionProcessor implements Processor {

    public static final String NAME = "sbtw";

    private final Processor source, left, right, greedy, caseSensitive;

    public BetweenFunctionProcessor(Processor source, Processor left, Processor right, Processor greedy, Processor caseSensitive) {
        this.source = source;
        this.left = left;
        this.right = right;
        this.greedy = greedy;
        this.caseSensitive = caseSensitive;
    }

    public BetweenFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        left = in.readNamedWriteable(Processor.class);
        right = in.readNamedWriteable(Processor.class);
        greedy = in.readNamedWriteable(Processor.class);
        caseSensitive = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
        out.writeNamedWriteable(greedy);
        out.writeNamedWriteable(caseSensitive);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), left.process(input), right.process(input),
                greedy.process(input), caseSensitive.process(input));
    }

    public static Object doProcess(Object source, Object left, Object right, Object greedy, Object caseSensitive) {
        if (source == null) {
            return null;
        }

        Check.isString(source);
        Check.isString(left);
        Check.isString(right);

        Check.isBoolean(greedy);
        Check.isBoolean(caseSensitive);

        String str = source.toString();
        String strRight = right.toString();
        String strLeft = left.toString();
        boolean bGreedy = ((Boolean) greedy).booleanValue();
        boolean bCaseSensitive = ((Boolean) caseSensitive).booleanValue();
        return StringUtils.between(str, strLeft, strRight, bGreedy, bCaseSensitive);
    }

    protected Processor source() {
        return source;
    }

    public Processor left() {
        return left;
    }

    public Processor right() {
        return right;
    }

    public Processor greedy() {
        return greedy;
    }

    public Processor caseSensitive() {
        return caseSensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), left(), right(), greedy(), caseSensitive());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BetweenFunctionProcessor other = (BetweenFunctionProcessor) obj;
        return Objects.equals(source(), other.source())
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right())
                && Objects.equals(greedy(), other.greedy())
                && Objects.equals(caseSensitive(), other.caseSensitive());
    }
}
