/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.printer;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.Decorator.Decoration;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.UnaryType;
import org.elasticsearch.painless.symbol.Decorations.BinaryType;
import org.elasticsearch.painless.symbol.Decorations.ShiftType;
import org.elasticsearch.painless.symbol.Decorations.ComparisonType;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.UpcastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessField;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessConstructor;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.SetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.StandardLocalFunction;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessClassBinding;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessInstanceBinding;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.ReturnType;
import org.elasticsearch.painless.symbol.Decorations.TypeParameters;
import org.elasticsearch.painless.symbol.Decorations.ParameterNames;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.InstanceType;
import org.elasticsearch.painless.symbol.Decorations.AccessDepth;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.Decorations.Converter;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DecorationToXContent {
    // PainlessCast
    // PainlessField
    // PainlessMethod
    // PainlessClassBinding
    // LocalFunction

    static final class Fields {
        static final String DECORATION = "decoration";
        static final String TYPE = "type";
    }

    public static void ToXContent(TargetType targetType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, targetType.getClass().getSimpleName());
        scope.field(Fields.TYPE, targetType.getTargetType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ValueType valueType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, valueType.getClass().getSimpleName());
        scope.field(Fields.TYPE, valueType.getValueType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(StaticType staticType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, staticType.getClass().getSimpleName());
        scope.field(Fields.TYPE, staticType.getStaticType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(PartialCanonicalTypeName partialCanonicalTypeName, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, partialCanonicalTypeName.getClass().getSimpleName());
        scope.field("partialCanonicalTypeName", partialCanonicalTypeName.getPartialCanonicalTypeName());
        scope.endObject();
    }

    public static void ToXContent(ExpressionPainlessCast expressionPainlessCast, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, expressionPainlessCast.getClass().getSimpleName());
        scope.field("cast");
        ToXContent(expressionPainlessCast.getExpressionPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(SemanticVariable semanticVariable, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, semanticVariable.getClass().getSimpleName());
        scope.field("variable");
        ToXContent(semanticVariable.getSemanticVariable(), scope);
        scope.endObject();
    }

    public static void ToXContent(IterablePainlessMethod iterablePainlessMethod, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, iterablePainlessMethod.getClass().getSimpleName());
        scope.field("painlessMethod");
        ToXContent(iterablePainlessMethod.getIterablePainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(UnaryType unaryType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, unaryType.getClass().getSimpleName());
        scope.field(Fields.TYPE, unaryType.getUnaryType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(BinaryType binaryType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, binaryType.getClass().getSimpleName());
        scope.field(Fields.TYPE, binaryType.getBinaryType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ShiftType shiftType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, shiftType.getClass().getSimpleName());
        scope.field(Fields.TYPE, shiftType.getShiftType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ComparisonType comparisonType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, comparisonType.getClass().getSimpleName());
        scope.field(Fields.TYPE, comparisonType.getComparisonType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(CompoundType compoundType, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, compoundType.getClass().getSimpleName());
        scope.field(Fields.TYPE, compoundType.getCompoundType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(UpcastPainlessCast upcastPainlessCast, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, upcastPainlessCast.getClass().getSimpleName());
        scope.field("cast");
        ToXContent(upcastPainlessCast.getUpcastPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(DowncastPainlessCast downcastPainlessCast, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, downcastPainlessCast.getClass().getSimpleName());
        scope.field("cast");
        ToXContent(downcastPainlessCast.getDowncastPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessField standardPainlessField, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, standardPainlessField.getClass().getSimpleName());
        scope.field("field");
        ToXContent(standardPainlessField.getStandardPainlessField(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessConstructor standardPainlessConstructor, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, standardPainlessConstructor.getClass().getSimpleName());
        scope.field("constructor");
        ToXContent(standardPainlessConstructor.getStandardPainlessConstructor(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessMethod standardPainlessMethod, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, standardPainlessMethod.getClass().getSimpleName());
        scope.field("method");
        ToXContent(standardPainlessMethod.getStandardPainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(GetterPainlessMethod getterPainlessMethod, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, getterPainlessMethod.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(SetterPainlessMethod setterPainlessMethod, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, setterPainlessMethod.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(StandardConstant standardConstant, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, standardConstant.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(StandardLocalFunction standardLocalFunction, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, standardLocalFunction.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(StandardPainlessClassBinding standardPainlessClassBinding, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, standardPainlessClassBinding.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(StandardPainlessInstanceBinding standardPainlessInstanceBinding, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, standardPainlessInstanceBinding.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(MethodNameDecoration methodNameDecoration, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, methodNameDecoration.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(ReturnType returnType, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, returnType.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(TypeParameters typeParameters, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, typeParameters.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(ParameterNames parameterNames, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, parameterNames.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(ReferenceDecoration referenceDecoration, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, referenceDecoration.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(EncodingDecoration encodingDecoration, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, encodingDecoration.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(CapturesDecoration capturesDecoration, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, capturesDecoration.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(InstanceType instanceType, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, instanceType.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(AccessDepth accessDepth, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, accessDepth.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(IRNodeDecoration irNodeDecoration, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, irNodeDecoration.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(Converter Converter, UserTreePrinterScope scope) {
        scope.startObject();

        scope.field(Fields.DECORATION, Converter.getClass().getSimpleName());

        scope.endObject();
    }

    public static void ToXContent(Decoration decoration, UserTreePrinterScope scope) {
        if  (decoration instanceof TargetType) {
            ToXContent((TargetType) decoration, scope);
        } else if (decoration instanceof ValueType) {
            ToXContent((ValueType) decoration, scope);
        } else if (decoration instanceof StaticType) {
            ToXContent((StaticType) decoration, scope);
        } else if (decoration instanceof PartialCanonicalTypeName) {
            ToXContent((PartialCanonicalTypeName) decoration, scope);
        } else if (decoration instanceof ExpressionPainlessCast) {
            ToXContent((ExpressionPainlessCast) decoration, scope);
        } else if (decoration instanceof SemanticVariable) {
            ToXContent((SemanticVariable) decoration, scope);
        } else if (decoration instanceof IterablePainlessMethod) {
            ToXContent((IterablePainlessMethod) decoration, scope);
        } else if (decoration instanceof UnaryType) {
            ToXContent((UnaryType) decoration, scope);
        } else if (decoration instanceof BinaryType) {
            ToXContent((BinaryType) decoration, scope);
        } else if (decoration instanceof ShiftType) {
            ToXContent((ShiftType) decoration, scope);
        } else if (decoration instanceof ComparisonType) {
            ToXContent((ComparisonType) decoration, scope);
        } else if (decoration instanceof CompoundType) {
            ToXContent((CompoundType) decoration, scope);
        } else if (decoration instanceof UpcastPainlessCast) {
            ToXContent((UpcastPainlessCast) decoration, scope);
        } else if (decoration instanceof DowncastPainlessCast) {
            ToXContent((DowncastPainlessCast) decoration, scope);
        } else if (decoration instanceof StandardPainlessField) {
            ToXContent((StandardPainlessField) decoration, scope);
        } else if (decoration instanceof StandardPainlessConstructor) {
            ToXContent((StandardPainlessConstructor) decoration, scope);
        } else if (decoration instanceof StandardPainlessMethod) {
            ToXContent((StandardPainlessMethod) decoration, scope);
        } else if (decoration instanceof GetterPainlessMethod) {
            ToXContent((GetterPainlessMethod) decoration, scope);
        } else if (decoration instanceof SetterPainlessMethod) {
            ToXContent((SetterPainlessMethod) decoration, scope);
        } else if (decoration instanceof StandardConstant) {
            ToXContent((StandardConstant) decoration, scope);
        } else if (decoration instanceof StandardLocalFunction) {
            ToXContent((StandardLocalFunction) decoration, scope);
        } else if (decoration instanceof StandardPainlessClassBinding) {
            ToXContent((StandardPainlessClassBinding) decoration, scope);
        } else if (decoration instanceof StandardPainlessInstanceBinding) {
            ToXContent((StandardPainlessInstanceBinding) decoration, scope);
        } else if (decoration instanceof MethodNameDecoration) {
            ToXContent((MethodNameDecoration) decoration, scope);
        } else if (decoration instanceof ReturnType) {
            ToXContent((ReturnType) decoration, scope);
        } else if (decoration instanceof TypeParameters) {
            ToXContent((TypeParameters) decoration, scope);
        } else if (decoration instanceof ParameterNames) {
            ToXContent((ParameterNames) decoration, scope);
        } else if (decoration instanceof ReferenceDecoration) {
            ToXContent((ReferenceDecoration) decoration, scope);
        } else if (decoration instanceof EncodingDecoration) {
            ToXContent((EncodingDecoration) decoration, scope);
        } else if (decoration instanceof CapturesDecoration) {
            ToXContent((CapturesDecoration) decoration, scope);
        } else if (decoration instanceof InstanceType) {
            ToXContent((InstanceType) decoration, scope);
        } else if (decoration instanceof AccessDepth) {
            ToXContent((AccessDepth) decoration, scope);
        } else if (decoration instanceof IRNodeDecoration) {
            ToXContent((IRNodeDecoration) decoration, scope);
        } else if (decoration instanceof Converter) {
            ToXContent((Converter) decoration, scope);
        } else {
            scope.startObject();
            scope.field(Fields.DECORATION, decoration.getClass().getSimpleName());
            scope.endObject();
        }
    }

    public static void ToXContent(PainlessCast painlessCast, UserTreePrinterScope scope) {
        scope.startObject();
        if (painlessCast.originalType != null) {
            scope.field("originalType", painlessCast.originalType.getSimpleName());
        }
        if (painlessCast.targetType != null) {
            scope.field("targetType", painlessCast.targetType.getSimpleName());
        }

        scope.field("explicitCast", painlessCast.explicitCast);

        if (painlessCast.unboxOriginalType != null) {
            scope.field("unboxOriginalType", painlessCast.unboxOriginalType.getSimpleName());
        }
        if (painlessCast.unboxTargetType != null) {
            scope.field("unboxTargetType", painlessCast.unboxTargetType.getSimpleName());
        }
        if (painlessCast.boxOriginalType != null) {
            scope.field("boxOriginalType", painlessCast.boxOriginalType.getSimpleName());
        }
        scope.endObject();
    }

    public static void ToXContent(SemanticScope.Variable variable, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.TYPE, variable.getType());
        scope.field("name", variable.getName());
        scope.field("isFinal", variable.isFinal());
        scope.endObject();
    }

    public static void ToXContent(PainlessMethod method, UserTreePrinterScope scope) {
        scope.startObject();
        if (method.javaMethod != null) {
            scope.field("javaMethod");
            ToXContent(method.methodType, scope);
        }
        if (method.targetClass != null) {
            scope.field("targetClass", method.targetClass.getSimpleName());
        }
        if (method.returnType != null) {
            scope.field("returnType", method.returnType.getSimpleName());
        }
        if (method.typeParameters != null) {
            scope.field("typeParameters", classNames(method.typeParameters));
        }
        if (method.methodHandle != null) {
            scope.field("methodHandle");
            ToXContent(method.methodHandle.type(), scope);
        }
        // ignoring methodType as that's handled under methodHandle
        if (method.annotations != null) {
            scope.field("annotations",
                    method.annotations.keySet().stream().map(Class::getSimpleName).sorted().collect(Collectors.toList())
            );
        }
        scope.endObject();
    }

    public static void ToXContent(PainlessField field, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("javaField");
        ToXContent(field.javaField, scope);
        scope.field("typeParameter", field.typeParameter.getSimpleName());
        scope.field("getterMethodHandle");
        ToXContent(field.getterMethodHandle.type(), scope);
        scope.field("setterMethodHandle");
        if (field.setterMethodHandle != null) {
            ToXContent(field.setterMethodHandle.type(), scope);
        }
        scope.endObject();
    }

    public static void ToXContent(PainlessConstructor constructor, UserTreePrinterScope scope) {
        scope.startObject();
        Class<?>[] parameterTypes = constructor.javaConstructor.getParameterTypes();
        if (parameterTypes.length > 0) {
            scope.field("javaConstructor", classNames(parameterTypes));
        }
        if (constructor.typeParameters.isEmpty() == false) {
            scope.field("typeParameters", classNames(constructor.typeParameters));
        }
        scope.field("methodHandle");
        ToXContent(constructor.methodHandle.type(), scope);
        scope.endObject();
    }

    public static void ToXContent(MethodType methodType, UserTreePrinterScope scope) {
        scope.startObject();
        List<Class<?>> parameters = methodType.parameterList();
        if (parameters.isEmpty() == false) {
            scope.field("parameters", classNames(parameters));
        }
        scope.field("return", methodType.returnType().getSimpleName());
        scope.endObject();
    }

    // java.lang.reflect
    public static void ToXContent(Field field, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("name", field.getName());
        scope.field("type", field.getType().getSimpleName());
        scope.field("modifiers", Modifier.toString(field.getModifiers()));
        scope.endObject();
    }

    public static void ToXContent(Method method, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("name", method.getName());
        scope.field("parameters", classNames(method.getParameterTypes()));
        scope.field("return", method.getReturnType().getSimpleName());
        Class<?>[] exceptions = method.getExceptionTypes();
        if (exceptions.length > 0) {
            scope.field("exceptions", classNames(exceptions));
        }
        scope.field("modifiers", Modifier.toString(method.getModifiers()));
        scope.endObject();
    }

    public static List<String> classNames(Class<?>[] classes) {
        return Arrays.stream(classes).map(Class::getSimpleName).collect(Collectors.toList());
    }

    public static List<String> classNames(List<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName).collect(Collectors.toList());
    }
}
