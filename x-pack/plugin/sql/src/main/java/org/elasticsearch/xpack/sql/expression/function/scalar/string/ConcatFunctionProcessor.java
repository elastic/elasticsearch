/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

public class ConcatFunctionProcessor extends BinaryProcessor {

    public static final String NAME = "scon";

    public ConcatFunctionProcessor(Processor source1, Processor source2) {
        super(source1, source2);
    }
    
    public ConcatFunctionProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Object doProcess(Object source1, Object source2) {
        return doProcessInScripts(source1, source2);
    }
    
    /**
     * Used in Painless scripting
     */
    public static Object doProcessInScripts(Object source1, Object source2) {
        if (source1 == null) {
            return source2;
        }
        if (source2 == null) {
            return source1;
        }
        if (!(source1 instanceof String || source1 instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source1);
        }
        if (!(source2 instanceof String || source2 instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source2);
        }

        return source1.toString().concat(source2.toString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        ConcatFunctionProcessor other = (ConcatFunctionProcessor) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
    }
}
