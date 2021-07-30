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
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.Decorator.Condition;
import org.elasticsearch.painless.symbol.Decorator.Decoration;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Decorations {

    // standard input for user statement nodes during semantic phase

    public interface LastSource extends Condition {

    }

    public interface BeginLoop extends Condition {

    }

    public interface InLoop extends Condition {

    }

    public interface LastLoop extends Condition {

    }

    // standard output for user statement nodes during semantic phase

    public interface MethodEscape extends Condition {

    }

    public interface LoopEscape extends Condition {

    }

    public interface AllEscape extends Condition {

    }

    public interface AnyContinue extends Condition {

    }

    public interface AnyBreak extends Condition {

    }

    // standard input for user expression nodes during semantic phase

    public interface Read extends Condition {

    }

    public interface Write extends Condition {

    }

    public static class TargetType implements Decoration  {

        private final Class<?> targetType;

        public TargetType(Class<?> targetType) {
            this.targetType = Objects.requireNonNull(targetType);
        }

        public Class<?> getTargetType() {
            return targetType;
        }

        public String getTargetCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(targetType);
        }
    }

    public interface Explicit extends Condition {

    }

    public interface Internal extends Condition {

    }

    // standard output for user expression node during semantic phase

    public static class ValueType implements Decoration {

        private final Class<?> valueType;

        public ValueType(Class<?> valueType) {
            this.valueType = Objects.requireNonNull(valueType);
        }

        public Class<?> getValueType() {
            return valueType;
        }

        public String getValueCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(valueType);
        }
    }

    public static class StaticType implements Decoration {

        private final Class<?> staticType;

        public StaticType(Class<?> staticType) {
            this.staticType = Objects.requireNonNull(staticType);
        }

        public Class<?> getStaticType() {
            return staticType;
        }

        public String getStaticCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(staticType);
        }
    }

    public static class PartialCanonicalTypeName implements Decoration {

        private final String partialCanonicalTypeName;

        public PartialCanonicalTypeName(String partialCanonicalTypeName) {
            this.partialCanonicalTypeName = Objects.requireNonNull(partialCanonicalTypeName);
        }

        public String getPartialCanonicalTypeName() {
            return partialCanonicalTypeName;
        }
    }

    public interface DefOptimized extends Condition {

    }

    // additional output acquired during the semantic process

    public interface ContinuousLoop extends Condition {

    }

    public interface Shortcut extends Condition {

    }

    public interface MapShortcut extends Condition {

    }

    public interface ListShortcut extends Condition {

    }

    public static class ExpressionPainlessCast implements Decoration {

        private final PainlessCast expressionPainlessCast;

        public ExpressionPainlessCast(PainlessCast expressionPainlessCast) {
            this.expressionPainlessCast = Objects.requireNonNull(expressionPainlessCast);
        }

        public PainlessCast getExpressionPainlessCast() {
            return expressionPainlessCast;
        }
    }

    public static class SemanticVariable implements Decoration {

        private final Variable semanticVariable;

        public SemanticVariable(Variable semanticVariable) {
            this.semanticVariable = semanticVariable;
        }

        public Variable getSemanticVariable() {
            return semanticVariable;
        }
    }

    public static class IterablePainlessMethod implements Decoration {

        private final PainlessMethod iterablePainlessMethod;

        public IterablePainlessMethod(PainlessMethod iterablePainlessMethod) {
            this.iterablePainlessMethod = Objects.requireNonNull(iterablePainlessMethod);
        }

        public PainlessMethod getIterablePainlessMethod() {
            return iterablePainlessMethod;
        }
    }

    public static class UnaryType implements Decoration {

        private final Class<?> unaryType;

        public UnaryType(Class<?> unaryType) {
            this.unaryType = Objects.requireNonNull(unaryType);
        }

        public Class<?> getUnaryType() {
            return unaryType;
        }

        public String getUnaryCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(unaryType);
        }
    }

    public static class BinaryType implements Decoration {

        private final Class<?> binaryType;

        public BinaryType(Class<?> binaryType) {
            this.binaryType = Objects.requireNonNull(binaryType);
        }

        public Class<?> getBinaryType() {
            return binaryType;
        }

        public String getBinaryCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(binaryType);
        }
    }

    public static class ShiftType implements Decoration {

        private final Class<?> shiftType;

        public ShiftType(Class<?> shiftType) {
            this.shiftType = Objects.requireNonNull(shiftType);
        }

        public Class<?> getShiftType() {
            return shiftType;
        }

        public String getShiftCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(shiftType);
        }
    }

    public static class ComparisonType implements Decoration {

        private final Class<?> comparisonType;

        public ComparisonType(Class<?> comparisonType) {
            this.comparisonType = Objects.requireNonNull(comparisonType);
        }

        public Class<?> getComparisonType() {
            return comparisonType;
        }

        public String getComparisonCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(comparisonType);
        }
    }

    public static class CompoundType implements Decoration {

        private final Class<?> compoundType;

        public CompoundType(Class<?> compoundType) {
            this.compoundType = Objects.requireNonNull(compoundType);
        }

        public Class<?> getCompoundType() {
            return compoundType;
        }

        public String getCompoundCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(compoundType);
        }
    }

    public static class UpcastPainlessCast implements Decoration {

        private final PainlessCast upcastPainlessCast;

        public UpcastPainlessCast(PainlessCast upcastPainlessCast) {
            this.upcastPainlessCast = Objects.requireNonNull(upcastPainlessCast);
        }

        public PainlessCast getUpcastPainlessCast() {
            return upcastPainlessCast;
        }
    }

    public static class DowncastPainlessCast implements Decoration {

        private final PainlessCast downcastPainlessCast;

        public DowncastPainlessCast(PainlessCast downcastPainlessCast) {
            this.downcastPainlessCast = Objects.requireNonNull(downcastPainlessCast);
        }

        public PainlessCast getDowncastPainlessCast() {
            return downcastPainlessCast;
        }
    }

    public static class StandardPainlessField implements Decoration {

        private final PainlessField standardPainlessField;

        public StandardPainlessField(PainlessField standardPainlessField) {
            this.standardPainlessField = Objects.requireNonNull(standardPainlessField);
        }

        public PainlessField getStandardPainlessField() {
            return standardPainlessField;
        }
    }

    public static class StandardPainlessConstructor implements Decoration {

        private final PainlessConstructor standardPainlessConstructor;

        public StandardPainlessConstructor(PainlessConstructor standardPainlessConstructor) {
            this.standardPainlessConstructor = Objects.requireNonNull(standardPainlessConstructor);
        }

        public PainlessConstructor getStandardPainlessConstructor() {
            return standardPainlessConstructor;
        }
    }

    public static class StandardPainlessMethod implements Decoration {

        private final PainlessMethod standardPainlessMethod;

        public StandardPainlessMethod(PainlessMethod standardPainlessMethod) {
            this.standardPainlessMethod = Objects.requireNonNull(standardPainlessMethod);
        }

        public PainlessMethod getStandardPainlessMethod() {
            return standardPainlessMethod;
        }
    }

    public static class GetterPainlessMethod implements Decoration {

        private final PainlessMethod getterPainlessMethod;

        public GetterPainlessMethod(PainlessMethod getterPainlessMethod) {
            this.getterPainlessMethod = Objects.requireNonNull(getterPainlessMethod);
        }

        public PainlessMethod getGetterPainlessMethod() {
            return getterPainlessMethod;
        }
    }

    public static class SetterPainlessMethod implements Decoration {

        private final PainlessMethod setterPainlessMethod;

        public SetterPainlessMethod(PainlessMethod setterPainlessMethod) {
            this.setterPainlessMethod = Objects.requireNonNull(setterPainlessMethod);
        }

        public PainlessMethod getSetterPainlessMethod() {
            return setterPainlessMethod;
        }
    }

    public static class StandardConstant implements Decoration {

        private final Object standardConstant;

        public StandardConstant(Object standardConstant) {
            this.standardConstant = Objects.requireNonNull(standardConstant);
        }

        public Object getStandardConstant() {
            return standardConstant;
        }
    }

    public static class StandardLocalFunction implements Decoration {

        private final LocalFunction localFunction;

        public StandardLocalFunction(LocalFunction localFunction) {
            this.localFunction = Objects.requireNonNull(localFunction);
        }

        public LocalFunction getLocalFunction() {
            return localFunction;
        }
    }

    public static class StandardPainlessClassBinding implements Decoration {

        private final PainlessClassBinding painlessClassBinding;

        public StandardPainlessClassBinding(PainlessClassBinding painlessClassBinding) {
            this.painlessClassBinding = Objects.requireNonNull(painlessClassBinding);
        }

        public PainlessClassBinding getPainlessClassBinding() {
            return painlessClassBinding;
        }
    }

    public static class StandardPainlessInstanceBinding implements Decoration {

        private final PainlessInstanceBinding painlessInstanceBinding;

        public StandardPainlessInstanceBinding(PainlessInstanceBinding painlessInstanceBinding) {
            this.painlessInstanceBinding = Objects.requireNonNull(painlessInstanceBinding);
        }

        public PainlessInstanceBinding getPainlessInstanceBinding() {
            return painlessInstanceBinding;
        }
    }

    public static class MethodNameDecoration implements Decoration {

        private final String methodName;

        public MethodNameDecoration(String methodName) {
            this.methodName = Objects.requireNonNull(methodName);
        }

        public String getMethodName() {
            return methodName;
        }
    }

    public static class ReturnType implements Decoration {

        private final Class<?> returnType;

        public ReturnType(Class<?> returnType) {
            this.returnType = Objects.requireNonNull(returnType);
        }

        public Class<?> getReturnType() {
            return returnType;
        }

        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(returnType);
        }
    }

    public static class TypeParameters implements Decoration {

        private final List<Class<?>> typeParameters;

        public TypeParameters(List<Class<?>> typeParameters) {
            this.typeParameters = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(typeParameters)));
        }

        public List<Class<?>> getTypeParameters() {
            return typeParameters;
        }
    }

    public static class ParameterNames implements Decoration {

        private final List<String> parameterNames;

        public ParameterNames(List<String> parameterNames) {
            this.parameterNames = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(parameterNames)));
        }

        public List<String> getParameterNames() {
            return parameterNames;
        }
    }

    public static class ReferenceDecoration implements Decoration {

        private final FunctionRef reference;

        public ReferenceDecoration(FunctionRef reference) {
            this.reference = Objects.requireNonNull(reference);
        }

        public FunctionRef getReference() {
            return reference;
        }
    }

    public static class EncodingDecoration implements Decoration {

        private final Def.Encoding encoding;

        public EncodingDecoration(boolean isStatic, boolean needsInstance, String symbol, String methodName, int captures) {
            this.encoding = new Def.Encoding(isStatic, needsInstance, symbol, methodName, captures);
        }

        public Def.Encoding getEncoding() {
            return encoding;
        }
    }

    public static class CapturesDecoration implements Decoration {

        private final List<Variable> captures;

        public CapturesDecoration(List<Variable> captures) {
            this.captures = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(captures)));
        }

        public List<Variable> getCaptures() {
            return captures;
        }
    }

    public interface CaptureBox extends Condition {

    }

    public static class InstanceType implements Decoration {

        private final Class<?> instanceType;

        public InstanceType(Class<?> instanceType) {
            this.instanceType = Objects.requireNonNull(instanceType);
        }

        public Class<?> getInstanceType() {
            return instanceType;
        }

        public String getInstanceCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(instanceType);
        }
    }

    public interface Negate extends Condition {

    }

    public interface Compound extends Condition {

    }

    public static class AccessDepth implements Decoration {

        private final int accessDepth;

        public AccessDepth(int accessDepth) {
            this.accessDepth = accessDepth;
        }

        public int getAccessDepth() {
            return accessDepth;
        }
    }

    // standard output for user tree to ir tree phase

    public static class IRNodeDecoration implements Decoration {

        private final IRNode irNode;

        public IRNodeDecoration(IRNode irNode) {
            this.irNode = Objects.requireNonNull(irNode);
        }

        public IRNode getIRNode() {
            return irNode;
        }
    }

    public static class Converter implements Decoration {
        private final LocalFunction converter;
        public Converter(LocalFunction converter) {
            this.converter = converter;
        }

        public LocalFunction getConverter() {
            return converter;
        }
    }

    // collect additional information about where doc is used

    public interface IsDocument extends Condition {

    }

    // Does the lambda need to capture the enclosing instance?
    public interface InstanceCapturingLambda extends Condition {

    }

    // Does the function reference need to capture the enclosing instance?
    public interface InstanceCapturingFunctionRef extends Condition {

    }
}
