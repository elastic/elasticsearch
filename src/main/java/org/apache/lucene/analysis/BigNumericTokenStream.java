/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.analysis;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.*;

import java.math.BigInteger;


/**
 * Modified from {@link NumericTokenStream} to support {@link java.math.BigInteger}
 */
public final class BigNumericTokenStream extends TokenStream {
    public static final String TOKEN_TYPE_FULL_PREC  = "fullPrecNumeric";

    public static final String TOKEN_TYPE_LOWER_PREC = "lowerPrecNumeric";

    public interface NumericTermAttribute extends Attribute {
        int getShift();
        BigInteger getRawValue();
        int getValueSize();
        void init(BigInteger value, int valueSize, int precisionStep, int shift);
        void setShift(int shift);
        int incShift();
    }

    // just a wrapper to prevent adding CTA
    private static final class NumericAttributeFactory extends AttributeFactory {
        private final AttributeFactory delegate;

        NumericAttributeFactory(AttributeFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
            if (CharTermAttribute.class.isAssignableFrom(attClass))
                throw new IllegalArgumentException("NumericTokenStream does not support CharTermAttribute.");
            return delegate.createAttributeInstance(attClass);
        }
    }

    public static final class NumericTermAttributeImpl extends AttributeImpl implements NumericTermAttribute,TermToBytesRefAttribute {
        private BigInteger value;
        private int valueSize = 0, shift = 0, precisionStep = 0;
        private BytesRef bytes = new BytesRef();

        public NumericTermAttributeImpl() {}

        @Override
        public BytesRef getBytesRef() {
            return bytes;
        }

        @Override
        public int fillBytesRef() {
            return BigNumericUtils.bigIntToPrefixCoded(value, shift, bytes, valueSize);
        }

        @Override
        public int getShift() { return shift; }
        @Override
        public void setShift(int shift) { this.shift = shift; }
        @Override
        public int incShift() {
            return (shift += precisionStep);
        }

        @Override
        public BigInteger getRawValue() { return value; }
        @Override
        public int getValueSize() { return valueSize; }

        @Override
        public void init(BigInteger value, int valueSize, int precisionStep, int shift) {
            this.value = value;
            this.valueSize = valueSize;
            this.precisionStep = precisionStep;
            this.shift = shift;
        }

        @Override
        public void clear() {
        }

        @Override
        public void reflectWith(AttributeReflector reflector) {
            fillBytesRef();
            reflector.reflect(TermToBytesRefAttribute.class, "bytes", BytesRef.deepCopyOf(bytes));
            reflector.reflect(NumericTermAttribute.class, "shift", shift);
            reflector.reflect(NumericTermAttribute.class, "rawValue", getRawValue());
            reflector.reflect(NumericTermAttribute.class, "valueSize", valueSize);
        }

        @Override
        public void copyTo(AttributeImpl target) {
            final NumericTermAttribute a = (NumericTermAttribute) target;
            a.init(value, valueSize, precisionStep, shift);
        }
    }

    public BigNumericTokenStream() {
        this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, NumericUtils.PRECISION_STEP_DEFAULT, BigNumericUtils.VALUE_SIZE_DEFAULT);
    }


    public BigNumericTokenStream(final int precisionStep) {
        this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, precisionStep, BigNumericUtils.VALUE_SIZE_DEFAULT);
    }

    public BigNumericTokenStream(final int precisionStep, final int valSize) {
        this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, precisionStep, valSize);
    }

    public BigNumericTokenStream(AttributeFactory factory, final int precisionStep, final int valSize) {
        super(new NumericAttributeFactory(factory));
        if (precisionStep < 1)
            throw new IllegalArgumentException("precisionStep must be >=1");
        this.precisionStep = precisionStep;
        this.valSize = valSize;
        numericAtt.setShift(-precisionStep);
    }

    public BigNumericTokenStream setBigIntValue(final BigInteger value) {
        numericAtt.init(value, valSize, precisionStep, -precisionStep);
        return this;
    }

    @Override
    public void reset() {
        if (valSize == 0)
            throw new IllegalStateException("call set???Value() before usage");
        numericAtt.setShift(-precisionStep);
    }

    @Override
    public boolean incrementToken() {
        if (valSize == 0)
            throw new IllegalStateException("call set???Value() before usage");

        // this will only clear all other attributes in this TokenStream
        clearAttributes();

        final int shift = numericAtt.incShift();
        typeAtt.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC : TOKEN_TYPE_LOWER_PREC);
        posIncrAtt.setPositionIncrement((shift == 0) ? 1 : 0);
        return (shift < valSize);
    }

    /** Returns the precision step. */
    public int getPrecisionStep() {
        return precisionStep;
    }

    // members
    private final NumericTermAttribute numericAtt = addAttribute(NumericTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    private int valSize = 0; // valSize==0 means not initialized
    private final int precisionStep;
}
