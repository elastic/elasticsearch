/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues.Booleans;
import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.index.fielddata.ScriptDocValues.Doubles;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.IpFieldMapper.IpFieldType.IpScriptDocValues;

public abstract class ToScriptField {

    // TODO: move these to appropriate field types as they are created (https://github.com/elastic/elasticsearch/issues/79105)
    public static final class ToBooleanScriptField extends ToScriptField {

        public static final ToBooleanScriptField INSTANCE = new ToBooleanScriptField();

        private ToBooleanScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Booleans(sortedNumericDocValues), name);
        }
    }

    public static final class ToByteScriptField extends ToScriptField {

        public static final ToByteScriptField INSTANCE = new ToByteScriptField();

        private ToByteScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToShortScriptField extends ToScriptField {

        public static final ToShortScriptField INSTANCE = new ToShortScriptField();

        private ToShortScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToIntScriptField extends ToScriptField {

        public static final ToIntScriptField INSTANCE = new ToIntScriptField();

        private ToIntScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToLongScriptField extends ToScriptField {

        public static final ToLongScriptField INSTANCE = new ToLongScriptField();

        private ToLongScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToDateMillisScriptField extends ToScriptField {

        public static final ToDateMillisScriptField INSTANCE = new ToDateMillisScriptField();

        private ToDateMillisScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Dates(sortedNumericDocValues, false), name);
        }
    }

    public static class ToDateNanosScriptField extends ToScriptField {

        public static final ToDateNanosScriptField INSTANCE = new ToDateNanosScriptField();

        private ToDateNanosScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Dates(sortedNumericDocValues, true), name);
        }
    }

    public static class ToHalfFloatScriptField extends ToScriptField {

        public static final ToHalfFloatScriptField INSTANCE = new ToHalfFloatScriptField();

        private ToHalfFloatScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToScaledFloatField extends ToScriptField {

        public static final ToScaledFloatField INSTANCE = new ToScaledFloatField();

        private ToScaledFloatField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToFloatScriptField extends ToScriptField {

        public static final ToFloatScriptField INSTANCE = new ToFloatScriptField();

        private ToFloatScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToDoubleScriptField extends ToScriptField {

        public static final ToDoubleScriptField INSTANCE = new ToDoubleScriptField();

        private ToDoubleScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
            return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
        }
    }

    public static class ToSeqNoScriptField extends ToScriptField {

        public static final ToSeqNoScriptField INSTANCE = new ToSeqNoScriptField();

        private ToSeqNoScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToVersionScriptField extends ToScriptField {

        public static final ToVersionScriptField INSTANCE = new ToVersionScriptField();

        private ToVersionScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToKeywordScriptField extends ToScriptField {

        public static final ToKeywordScriptField INSTANCE = new ToKeywordScriptField();

        private ToKeywordScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToConstantKeywordScriptField extends ToScriptField {

        public static final ToConstantKeywordScriptField INSTANCE = new ToConstantKeywordScriptField();

        private ToConstantKeywordScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToWildcardScriptField extends ToScriptField {

        public static final ToWildcardScriptField INSTANCE = new ToWildcardScriptField();

        private ToWildcardScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToICUCollationKeywordScriptField extends ToScriptField {

        public static final ToICUCollationKeywordScriptField INSTANCE = new ToICUCollationKeywordScriptField();

        private ToICUCollationKeywordScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToIpScriptField extends ToScriptField {

        public static final ToIpScriptField INSTANCE = new ToIpScriptField();

        private ToIpScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return new DelegateDocValuesField(new IpScriptDocValues(sortedSetDocValues), name);
        }
    }

    public static class ToIdScriptField extends ToScriptField {

        public static final ToIdScriptField INSTANCE = new ToIdScriptField();

        private ToIdScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToParentIdScriptField extends ToScriptField {

        public static final ToParentIdScriptField INSTANCE = new ToParentIdScriptField();

        private ToParentIdScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToParentJoinScriptField extends ToScriptField {

        public static final ToParentJoinScriptField INSTANCE = new ToParentJoinScriptField();

        private ToParentJoinScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToIndexField extends ToScriptField {

        public static final ToIndexField INSTANCE = new ToIndexField();

        private ToIndexField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToMurmur3ScriptField extends ToScriptField {

        public static final ToMurmur3ScriptField INSTANCE = new ToMurmur3ScriptField();

        private ToMurmur3ScriptField() {

        }

        public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
            return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
        }
    }

    public static class ToTextScriptField extends ToScriptField {

        public static final ToTextScriptField INSTANCE = new ToTextScriptField();

        private ToTextScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToFlattenedScriptField extends ToScriptField {

        public static final ToFlattenedScriptField INSTANCE = new ToFlattenedScriptField();

        private ToFlattenedScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToKeyedFlattenedScriptField extends ToScriptField {

        public static final ToKeyedFlattenedScriptField INSTANCE = new ToKeyedFlattenedScriptField();

        private ToKeyedFlattenedScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public static class ToRootFlattenedScriptField extends ToScriptField {

        public static final ToRootFlattenedScriptField INSTANCE = new ToRootFlattenedScriptField();

        private ToRootFlattenedScriptField() {

        }

        @Override
        public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
            return new DelegateDocValuesField(new Strings(sortedBinaryDocValues), name);
        }

        @Override
        public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
            return getScriptField(FieldData.toString(sortedSetDocValues), name);
        }
    }

    public DocValuesField<?> getScriptField(SortedNumericDocValues sortedNumericDocValues, String name) {
        return new DelegateDocValuesField(new Longs(sortedNumericDocValues), name);
    }

    public DocValuesField<?> getScriptField(SortedNumericDoubleValues sortedNumericDoubleValues, String name) {
        return new DelegateDocValuesField(new Doubles(sortedNumericDoubleValues), name);
    }

    public DocValuesField<?> getScriptField(SortedBinaryDocValues sortedBinaryDocValues, String name) {
        return new BinaryDocValuesField(sortedBinaryDocValues, name);
    }

    public DocValuesField<?> getScriptField(SortedSetDocValues sortedSetDocValues, String name) {
        return new DelegateDocValuesField(new Strings(FieldData.toString(sortedSetDocValues)), name);
    }
}
