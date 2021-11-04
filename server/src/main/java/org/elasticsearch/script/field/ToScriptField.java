/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.index.fielddata.ScriptDocValues.Doubles;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

public abstract class ToScriptField {

    public static class ToUnsupportedScriptField extends ToScriptField {

        public static final ToUnsupportedScriptField INSTANCE = new ToUnsupportedScriptField();

        private ToUnsupportedScriptField() {

        }
    }

    public static class ToVersionScriptField extends ToScriptField {

        public static final ToVersionScriptField INSTANCE = new ToVersionScriptField();

        private ToVersionScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToSeqNoScriptField extends ToScriptField {

        public static final ToSeqNoScriptField INSTANCE = new ToSeqNoScriptField();

        private ToSeqNoScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToBooleanScriptField extends ToScriptField {

        public static final ToBooleanScriptField INSTANCE = new ToBooleanScriptField();

        private ToBooleanScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new BooleanDocValuesField(sortedNumericDocValues, name);
        }
    }

    public static class ToByteScriptField extends ToScriptField {

        public static final ToByteScriptField INSTANCE = new ToByteScriptField();

        private ToByteScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToShortScriptField extends ToScriptField {

        public static final ToShortScriptField INSTANCE = new ToShortScriptField();

        private ToShortScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToIntScriptField extends ToScriptField {

        public static final ToIntScriptField INSTANCE = new ToIntScriptField();

        private ToIntScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToLongScriptField extends ToScriptField {

        public static final ToLongScriptField INSTANCE = new ToLongScriptField();

        private ToLongScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToHalfFloatScriptField extends ToScriptField {

        public static final ToHalfFloatScriptField INSTANCE = new ToHalfFloatScriptField();

        private ToHalfFloatScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToFloatScriptField extends ToScriptField {

        public static final ToFloatScriptField INSTANCE = new ToFloatScriptField();

        private ToFloatScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToScaledFloatScriptField extends ToScriptField {

        public static final ToScaledFloatScriptField INSTANCE = new ToScaledFloatScriptField();

        private ToScaledFloatScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToDoubleScriptField extends ToScriptField {

        public static final ToDoubleScriptField INSTANCE = new ToDoubleScriptField();

        private ToDoubleScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToDateMillisScriptField extends ToScriptField {

        public static final ToDateMillisScriptField INSTANCE = new ToDateMillisScriptField();

        private ToDateMillisScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Dates(sortedNumericDocValues, false), name);
        }
    }

    public static class ToDateNanosScriptField extends ToScriptField {

        public static final ToDateNanosScriptField INSTANCE = new ToDateNanosScriptField();

        private ToDateNanosScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Dates(sortedNumericDocValues, true), name);
        }
    }

    public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
        throw new UnsupportedOperationException();
    }

    public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
        throw new UnsupportedOperationException();
    }
}
