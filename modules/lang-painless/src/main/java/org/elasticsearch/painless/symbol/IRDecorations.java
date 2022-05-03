/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.Def;
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

    /** base class for all type decorations to provide consistent {@code #toString()} */
    public abstract static class IRDType extends IRDecoration<Class<?>> {

        public IRDType(Class<?> value) {
            super(value);
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.typeToCanonicalTypeName(getValue());
        }
    }

    /** all expressions are decorated with a type based on the result */
    public static class IRDExpressionType extends IRDType {

        public IRDExpressionType(Class<?> value) {
            super(value);
        }
    }

    /** binary type is used as an optimization for binary math */
    public static class IRDBinaryType extends IRDType {

        public IRDBinaryType(Class<?> value) {
            super(value);
        }
    }

    /** shift type is used to define the right-hand side type of a shift operation */
    public static class IRDShiftType extends IRDType {

        public IRDShiftType(Class<?> value) {
            super(value);
        }
    }

    /** describes a math operation */
    public static class IRDOperation extends IRDecoration<Operation> {

        public IRDOperation(Operation value) {
            super(value);
        }

        @Override
        public String toString() {
            return getValue().symbol;
        }
    }

    /** general flags used to describe options on a node; options depend on node type*/
    public static class IRDFlags extends IRDecoration<Integer> {

        public IRDFlags(Integer value) {
            super(value);
        }
    }

    /** condition attached to a statement node when all logical paths escape */
    public static class IRCAllEscape implements IRCondition {

        private IRCAllEscape() {

        }
    }

    /** condition attached to describe a cast for a node */
    public static class IRDCast extends IRDecoration<PainlessCast> {

        public IRDCast(PainlessCast value) {
            super(value);
        }
    }

    /** describes what exception type is caught */
    public static class IRDExceptionType extends IRDType {

        public IRDExceptionType(Class<?> value) {
            super(value);
        }
    }

    /** describes a symbol for a node; what symbol represents depends on node type */
    public static class IRDSymbol extends IRDecoration<String> {

        public IRDSymbol(String value) {
            super(value);
        }
    }

    /**
     * additional information for the type of comparison;
     * comparison node's result is always boolean so this is required
     */
    public static class IRDComparisonType extends IRDType {

        public IRDComparisonType(Class<?> value) {
            super(value);
        }
    }

    /**
     * describes a constant for a node; what the constant is depends on the node
     */
    public static class IRDConstant extends IRDecoration<Object> {

        public IRDConstant(Object value) {
            super(value);
        }
    }

    /**
     * describes the field name holding a constant value.
     */
    public static class IRDConstantFieldName extends IRDecoration<String> {
        public IRDConstantFieldName(String value) {
            super(value);
        }
    }

    /**
     * describes the type for a declaration
     */
    public static class IRDDeclarationType extends IRDType {

        public IRDDeclarationType(Class<?> value) {
            super(value);
        }
    }

    /** describes a name for a specific piece of data in a node; the data depends on the node */
    public static class IRDName extends IRDecoration<String> {

        public IRDName(String value) {
            super(value);
        }
    }

    /** describes an encoding used to resolve references and lambdas at runtime */
    public static class IRDDefReferenceEncoding extends IRDecoration<Def.Encoding> {

        public IRDDefReferenceEncoding(Def.Encoding value) {
            super(value);
        }
    }

    /** describes the size of a dup instruction */
    public static class IRDSize extends IRDecoration<Integer> {

        public IRDSize(Integer value) {
            super(value);
        }
    }

    /** describes the depth of a dup instruction */
    public static class IRDDepth extends IRDecoration<Integer> {

        public IRDDepth(Integer value) {
            super(value);
        }
    }

    /** describes modifiers on a class, method, or field; depends on node type */
    public static class IRDModifiers extends IRDecoration<Integer> {

        public IRDModifiers(Integer value) {
            super(value);
        }
    }

    /** describes the type for a member field on a class */
    public static class IRDFieldType extends IRDType {

        public IRDFieldType(Class<?> value) {
            super(value);
        }
    }

    /** describes a type for a variable access */
    public static class IRDVariableType extends IRDType {

        public IRDVariableType(Class<?> value) {
            super(value);
        }
    }

    /** describes the name for a variable access */
    public static class IRDVariableName extends IRDecoration<String> {

        public IRDVariableName(String value) {
            super(value);
        }
    }

    /** describes the array variable type in a foreach loop */
    public static class IRDArrayType extends IRDType {

        public IRDArrayType(Class<?> value) {
            super(value);
        }
    }

    /** describes the array variable name in a foreach loop */
    public static class IRDArrayName extends IRDecoration<String> {

        public IRDArrayName(String value) {
            super(value);
        }
    }

    /** describes the index variable type in a foreach loop */
    public static class IRDIndexType extends IRDType {

        public IRDIndexType(Class<?> value) {
            super(value);
        }
    }

    /** describes the index variable name variable in a foreach loop */
    public static class IRDIndexName extends IRDecoration<String> {

        public IRDIndexName(String value) {
            super(value);
        }
    }

    /** describes the array type in a foreach loop */
    public static class IRDIndexedType extends IRDType {

        public IRDIndexedType(Class<?> value) {
            super(value);
        }
    }

    /** describes the iterable variable type in a foreach loop */
    public static class IRDIterableType extends IRDType {

        public IRDIterableType(Class<?> value) {
            super(value);
        }
    }

    /** describes the iterable name type in a foreach loop */
    public static class IRDIterableName extends IRDecoration<String> {

        public IRDIterableName(String value) {
            super(value);
        }
    }

    /** describes a method for a node; which method depends on node type */
    public static class IRDMethod extends IRDecoration<PainlessMethod> {

        public IRDMethod(PainlessMethod value) {
            super(value);
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.buildPainlessMethodKey(getValue().javaMethod().getName(), getValue().typeParameters().size());
        }
    }

    /** describes the return type for a statement node */
    public static class IRDReturnType extends IRDType {

        public IRDReturnType(Class<?> value) {
            super(value);
        }
    }

    /** describes the parameter types for a function node */
    public static class IRDTypeParameters extends IRDecoration<List<Class<?>>> {

        public IRDTypeParameters(List<Class<?>> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    /** describes the parameter names for a function node */
    public static class IRDParameterNames extends IRDecoration<List<String>> {

        public IRDParameterNames(List<String> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    /** describes if a method or field is static */
    public static class IRCStatic implements IRCondition {

        private IRCStatic() {

        }
    }

    /** describes if a method has variadic arguments */
    public static class IRCVarArgs implements IRCondition {

        private IRCVarArgs() {

        }
    }

    /** describes if a method or field is synthetic */
    public static class IRCSynthetic implements IRCondition {

        private IRCSynthetic() {

        }
    }

    /** describes if a method needs to capture the script "this" */
    public static class IRCInstanceCapture implements IRCondition {

        private IRCInstanceCapture() {

        }
    }

    /** describes the maximum number of loop iterations possible in a method */
    public static class IRDMaxLoopCounter extends IRDecoration<Integer> {

        public IRDMaxLoopCounter(Integer value) {
            super(value);
        }
    }

    /** describes the type for an instanceof instruction */
    public static class IRDInstanceType extends IRDType {

        public IRDInstanceType(Class<?> value) {
            super(value);
        }
    }

    /** describes the call to a generated function */
    public static class IRDFunction extends IRDecoration<LocalFunction> {

        public IRDFunction(LocalFunction value) {
            super(value);
        }
    }

    /** describes a method for a node on the script class; which method depends on node type */
    public static class IRDThisMethod extends IRDecoration<PainlessMethod> {

        public IRDThisMethod(PainlessMethod value) {
            super(value);
        }

        @Override
        public String toString() {
            return PainlessLookupUtility.buildPainlessMethodKey(getValue().javaMethod().getName(), getValue().typeParameters().size());
        }
    }

    /** describes the call to a class binding */
    public static class IRDClassBinding extends IRDecoration<PainlessClassBinding> {

        public IRDClassBinding(PainlessClassBinding value) {
            super(value);
        }
    }

    /** describes the call to an instance binding */
    public static class IRDInstanceBinding extends IRDecoration<PainlessInstanceBinding> {

        public IRDInstanceBinding(PainlessInstanceBinding value) {
            super(value);
        }
    }

    /** describes a constructor for a new instruction */
    public static class IRDConstructor extends IRDecoration<PainlessConstructor> {

        public IRDConstructor(PainlessConstructor value) {
            super(value);
        }
    }

    /** describes a value used as an index for shortcut field accesses */
    public static class IRDValue extends IRDecoration<String> {

        public IRDValue(String value) {
            super(value);
        }
    }

    /** describes the field for a field access */
    public static class IRDField extends IRDecoration<PainlessField> {

        public IRDField(PainlessField value) {
            super(value);
        }
    }

    /** describes if a loop has no escape paths */
    public static class IRCContinuous implements IRCondition {

        private IRCContinuous() {

        }
    }

    /** describes if a new array is an initializer */
    public static class IRCInitialize implements IRCondition {

        private IRCInitialize() {

        }
    }

    /** describes if an expression's value is read from */
    public static class IRCRead implements IRCondition {

        private IRCRead() {

        }
    }

    /** describes the names of all captured variables in a reference or lambda */
    public static class IRDCaptureNames extends IRDecoration<List<String>> {

        public IRDCaptureNames(List<String> value) {
            super(Collections.unmodifiableList(value));
        }
    }

    /** describes if the first capture of a method reference requires boxing */
    public interface IRCCaptureBox extends IRCondition {

    }

    /** describes the type of value stored in an assignment operation */
    public static class IRDStoreType extends IRDType {

        public IRDStoreType(Class<?> value) {
            super(value);
        }
    }

    /** unary type is used as an optimization for unary math */
    public static class IRDUnaryType extends IRDType {

        public IRDUnaryType(Class<?> value) {
            super(value);
        }
    }

    /** describes a function reference resolved at compile-time */
    public static class IRDReference extends IRDecoration<FunctionRef> {

        public IRDReference(FunctionRef value) {
            super(value);
        }
    }

    /** describes the limit of operations performed on a regex */
    public static class IRDRegexLimit extends IRDecoration<Integer> {

        public IRDRegexLimit(Integer value) {
            super(value);
        }
    }
}
