/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.IRNode.IRCondition;
import org.elasticsearch.painless.ir.IRNode.IRDecoration;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;

import java.util.Collections;
import java.util.List;

public class IRDecorations {

    public abstract static class IRDType extends IRDecoration<Class<?>> {

        public IRDType(Class<?> value) {
            super(value);
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.typeToCanonicalTypeName(getValue());
        }
    }

    public static class IRDExpressionType extends IRDType {

        public IRDExpressionType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDBinaryType extends IRDType {

        public IRDBinaryType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDShiftType extends IRDType {

        public IRDShiftType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDOperation extends IRDecoration<Operation> {

        public IRDOperation(Operation value) {
            super(value);
        }

        @Override
        public String toString() {
            return getValue().symbol;
        }
    }

    public static class IRDFlags extends IRDecoration<Integer> {

        public IRDFlags(Integer value) {
            super(value);
        }
    }

    public static class IRCAllEscape implements IRCondition {

        private IRCAllEscape() {

        }
    }

    public static class IRDCast extends IRDecoration<PainlessCast> {

        public IRDCast(PainlessCast value) {
            super(value);
        }
    }

    public static class IRDExceptionType extends IRDType {

        public IRDExceptionType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDSymbol extends IRDecoration<String> {

        public IRDSymbol(String value) {
            super(value);
        }
    }

    public static class IRDComparisonType extends IRDType {

        public IRDComparisonType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDConstant extends IRDecoration<Object> {

        public IRDConstant(Object value) {
            super(value);
        }
    }

    public static class IRDDeclarationType extends IRDType {

        public IRDDeclarationType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDName extends IRDecoration<String> {

        public IRDName(String value) {
            super(value);
        }
    }

    public static class IRDEncoding extends IRDecoration<String> {

        public IRDEncoding(String value) {
            super(value);
        }
    }

    public static class IRDSize extends IRDecoration<Integer> {

        public IRDSize(Integer value) {
            super(value);
        }
    }

    public static class IRDDepth extends IRDecoration<Integer> {

        public IRDDepth(Integer value) {
            super(value);
        }
    }

    public static class IRDModifiers extends IRDecoration<Integer> {

        public IRDModifiers(Integer value) {
            super(value);
        }
    }

    public static class IRDFieldType extends IRDType {

        public IRDFieldType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDVariableType extends IRDType {

        public IRDVariableType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDVariableName extends IRDecoration<String> {

        public IRDVariableName(String value) {
            super(value);
        }
    }

    public static class IRDArrayType extends IRDType {

        public IRDArrayType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDArrayName extends IRDecoration<String> {

        public IRDArrayName(String value) {
            super(value);
        }
    }

    public static class IRDIndexType extends IRDType {

        public IRDIndexType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDIndexName extends IRDecoration<String> {

        public IRDIndexName(String value) {
            super(value);
        }
    }

    public static class IRDIndexedType extends IRDType {

        public IRDIndexedType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDIterableType extends IRDType {

        public IRDIterableType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDIterableName extends IRDecoration<String> {

        public IRDIterableName(String value) {
            super(value);
        }
    }

    public static class IRDMethod extends IRDecoration<PainlessMethod> {

        public IRDMethod(PainlessMethod value) {
            super(value);
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.buildPainlessMethodKey(getValue().javaMethod.getName(), getValue().typeParameters.size());
        }
    }

    public static class IRDReturnType extends IRDType {

        public IRDReturnType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDTypeParameters extends IRDecoration<List<Class<?>>> {

        public IRDTypeParameters(List<Class<?>> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    public static class IRDParameterNames extends IRDecoration<List<String>> {

        public IRDParameterNames(List<String> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    public static class IRCStatic implements IRCondition {

        private IRCStatic() {

        }
    }

    public static class IRCVarArgs implements IRCondition {

        private IRCVarArgs() {

        }
    }

    public static class IRCSynthetic implements IRCondition {

        private IRCSynthetic() {

        }
    }

    public static class IRDMaxLoopCounter extends IRDecoration<Integer> {

        public IRDMaxLoopCounter(Integer value) {
            super(value);
        }
    }

    public static class IRDInstanceType extends IRDType {

        public IRDInstanceType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDFunction extends IRDecoration<LocalFunction> {

        public IRDFunction(LocalFunction value) {
            super(value);
        }
    }

    public static class IRDClassBinding extends IRDecoration<PainlessClassBinding> {

        public IRDClassBinding(PainlessClassBinding value) {
            super(value);
        }
    }

    public static class IRDInstanceBinding extends IRDecoration<PainlessInstanceBinding> {

        public IRDInstanceBinding(PainlessInstanceBinding value) {
            super(value);
        }
    }

    public static class IRDConstructor extends IRDecoration<PainlessConstructor> {

        public IRDConstructor(PainlessConstructor value) {
            super(value);
        }
    }

    public static class IRDValue extends IRDecoration<String> {

        public IRDValue(String value) {
            super(value);
        }
    }

    public static class IRDField extends IRDecoration<PainlessField> {

        public IRDField(PainlessField value) {
            super(value);
        }
    }

    public static class IRCContinuous implements IRCondition {

        private IRCContinuous() {

        }
    }

    public static class IRCInitialize implements IRCondition {

        private IRCInitialize() {

        }
    }

    public static class IRCRead implements IRCondition {

        private IRCRead() {

        }
    }

    public static class IRDCaptureNames extends IRDecoration<List<String>> {

        public IRDCaptureNames(List<String> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    public static class IRDStoreType extends IRDType {

        public IRDStoreType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDUnaryType extends IRDType {

        public IRDUnaryType(Class<?> value) {
            super(value);
        }
    }

    public static class IRDReference extends IRDecoration<FunctionRef> {

        public IRDReference(FunctionRef value) {
            super(value);
        }
    }
}
