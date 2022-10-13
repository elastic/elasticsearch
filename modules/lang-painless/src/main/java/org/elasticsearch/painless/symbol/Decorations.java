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

    public record TargetType(Class<?> targetType) implements Decoration {

        public String getTargetCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(targetType);
        }
    }

    public interface Explicit extends Condition {

    }

    public interface Internal extends Condition {

    }

    // standard output for user expression node during semantic phase

    public record ValueType(Class<?> valueType) implements Decoration {

        public String getValueCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(valueType);
        }
    }

    public record StaticType(Class<?> staticType) implements Decoration {

        public String getStaticCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(staticType);
        }
    }

    public record PartialCanonicalTypeName(String partialCanonicalTypeName) implements Decoration {

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

    public record ExpressionPainlessCast(PainlessCast expressionPainlessCast) implements Decoration {}

    public record SemanticVariable(Variable semanticVariable) implements Decoration {}

    public record IterablePainlessMethod(PainlessMethod iterablePainlessMethod) implements Decoration {}

    public record UnaryType(Class<?> unaryType) implements Decoration {

        public String getUnaryCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(unaryType);
        }
    }

    public record BinaryType(Class<?> binaryType) implements Decoration {

        public String getBinaryCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(binaryType);
        }
    }

    public record ShiftType(Class<?> shiftType) implements Decoration {

        public String getShiftCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(shiftType);
        }
    }

    public record ComparisonType(Class<?> comparisonType) implements Decoration {

        public String getComparisonCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(comparisonType);
        }
    }

    public record CompoundType(Class<?> compoundType) implements Decoration {

        public String getCompoundCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(compoundType);
        }
    }

    public record UpcastPainlessCast(PainlessCast upcastPainlessCast) implements Decoration {}

    public record DowncastPainlessCast(PainlessCast downcastPainlessCast) implements Decoration {

        public DowncastPainlessCast(PainlessCast downcastPainlessCast) {
            this.downcastPainlessCast = Objects.requireNonNull(downcastPainlessCast);
        }
    }

    public record StandardPainlessField(PainlessField standardPainlessField) implements Decoration {}

    public record StandardPainlessConstructor(PainlessConstructor standardPainlessConstructor) implements Decoration {}

    public record StandardPainlessMethod(PainlessMethod standardPainlessMethod) implements Decoration {}

    public interface DynamicInvocation extends Condition {}

    public record GetterPainlessMethod(PainlessMethod getterPainlessMethod) implements Decoration {}

    public record SetterPainlessMethod(PainlessMethod setterPainlessMethod) implements Decoration {}

    public record StandardConstant(Object standardConstant) implements Decoration {}

    public record StandardLocalFunction(LocalFunction localFunction) implements Decoration {}

    public record ThisPainlessMethod(PainlessMethod thisPainlessMethod) implements Decoration {}

    public record StandardPainlessClassBinding(PainlessClassBinding painlessClassBinding) implements Decoration {}

    public record StandardPainlessInstanceBinding(PainlessInstanceBinding painlessInstanceBinding) implements Decoration {

    }

    public record MethodNameDecoration(String methodName) implements Decoration {

    }

    public record ReturnType(Class<?> returnType) implements Decoration {

        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(returnType);
        }
    }

    public record TypeParameters(List<Class<?>> typeParameters) implements Decoration {

        public TypeParameters(List<Class<?>> typeParameters) {
            this.typeParameters = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(typeParameters)));
        }
    }

    public record ParameterNames(List<String> parameterNames) implements Decoration {

        public ParameterNames(List<String> parameterNames) {
            this.parameterNames = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(parameterNames)));
        }
    }

    public record ReferenceDecoration(FunctionRef reference) implements Decoration {}

    public record EncodingDecoration(Def.Encoding encoding) implements Decoration {

        public static EncodingDecoration of(boolean isStatic, boolean needsInstance, String symbol, String methodName, int captures) {
            return new EncodingDecoration(new Def.Encoding(isStatic, needsInstance, symbol, methodName, captures));
        }
    }

    public record CapturesDecoration(List<Variable> captures) implements Decoration {

        public CapturesDecoration(List<Variable> captures) {
            this.captures = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(captures)));
        }

        public List<Variable> captures() {
            return captures;
        }
    }

    public interface CaptureBox extends Condition {

    }

    public record InstanceType(Class<?> instanceType) implements Decoration {

        public String getInstanceCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(instanceType);
        }
    }

    public interface Negate extends Condition {

    }

    public interface Compound extends Condition {

    }

    public record AccessDepth(int accessDepth) implements Decoration {}

    // standard output for user tree to ir tree phase

    public record IRNodeDecoration(IRNode irNode) implements Decoration {}

    public record Converter(LocalFunction converter) implements Decoration {}

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
