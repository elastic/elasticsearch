/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.printer;

import org.elasticsearch.Build;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.painless.spi.annotation.DeprecatedAnnotation;
import org.elasticsearch.painless.spi.annotation.InjectConstantAnnotation;
import org.elasticsearch.painless.spi.annotation.NoImportAnnotation;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
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
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        start(targetType, scope);
        scope.field(Fields.TYPE, targetType.getTargetType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ValueType valueType, UserTreePrinterScope scope) {
        start(valueType, scope);
        scope.field(Fields.TYPE, valueType.getValueType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(StaticType staticType, UserTreePrinterScope scope) {
        start(staticType, scope);
        scope.field(Fields.TYPE, staticType.getStaticType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(PartialCanonicalTypeName partialCanonicalTypeName, UserTreePrinterScope scope) {
        start(partialCanonicalTypeName, scope);
        scope.field(Fields.TYPE, partialCanonicalTypeName.getPartialCanonicalTypeName());
        scope.endObject();
    }

    public static void ToXContent(ExpressionPainlessCast expressionPainlessCast, UserTreePrinterScope scope) {
        start(expressionPainlessCast, scope);
        scope.field("cast");
        ToXContent(expressionPainlessCast.getExpressionPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(SemanticVariable semanticVariable, UserTreePrinterScope scope) {
        start(semanticVariable, scope);
        scope.field("variable");
        ToXContent(semanticVariable.getSemanticVariable(), scope);
        scope.endObject();
    }

    public static void ToXContent(IterablePainlessMethod iterablePainlessMethod, UserTreePrinterScope scope) {
        start(iterablePainlessMethod, scope);
        scope.field("method");
        ToXContent(iterablePainlessMethod.getIterablePainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(UnaryType unaryType, UserTreePrinterScope scope) {
        start(unaryType, scope);
        scope.field(Fields.TYPE, unaryType.getUnaryType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(BinaryType binaryType, UserTreePrinterScope scope) {
        start(binaryType, scope);
        scope.field(Fields.TYPE, binaryType.getBinaryType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ShiftType shiftType, UserTreePrinterScope scope) {
        start(shiftType, scope);
        scope.field(Fields.TYPE, shiftType.getShiftType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(ComparisonType comparisonType, UserTreePrinterScope scope) {
        start(comparisonType, scope);
        scope.field(Fields.TYPE, comparisonType.getComparisonType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(CompoundType compoundType, UserTreePrinterScope scope) {
        start(compoundType, scope);
        scope.field(Fields.TYPE, compoundType.getCompoundType().getSimpleName());
        scope.endObject();
    }

    public static void ToXContent(UpcastPainlessCast upcastPainlessCast, UserTreePrinterScope scope) {
        start(upcastPainlessCast, scope);
        scope.field("cast");
        ToXContent(upcastPainlessCast.getUpcastPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(DowncastPainlessCast downcastPainlessCast, UserTreePrinterScope scope) {
        start(downcastPainlessCast, scope);
        scope.field("cast");
        ToXContent(downcastPainlessCast.getDowncastPainlessCast(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessField standardPainlessField, UserTreePrinterScope scope) {
        start(standardPainlessField, scope);
        scope.field("field");
        ToXContent(standardPainlessField.getStandardPainlessField(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessConstructor standardPainlessConstructor, UserTreePrinterScope scope) {
        start(standardPainlessConstructor, scope);
        scope.field("constructor");
        ToXContent(standardPainlessConstructor.getStandardPainlessConstructor(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessMethod standardPainlessMethod, UserTreePrinterScope scope) {
        start(standardPainlessMethod, scope);
        scope.field("method");
        ToXContent(standardPainlessMethod.getStandardPainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(GetterPainlessMethod getterPainlessMethod, UserTreePrinterScope scope) {
        start(getterPainlessMethod, scope);
        scope.field("method");
        ToXContent(getterPainlessMethod.getGetterPainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(SetterPainlessMethod setterPainlessMethod, UserTreePrinterScope scope) {
        start(setterPainlessMethod, scope);
        scope.field("method");
        ToXContent(setterPainlessMethod.getSetterPainlessMethod(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardConstant standardConstant, UserTreePrinterScope scope) {
        start(standardConstant, scope);
        scope.startObject("constant");
        scope.field(Fields.TYPE, standardConstant.getStandardConstant().getClass().getSimpleName());
        scope.field("value", standardConstant.getStandardConstant());
        scope.endObject();
        scope.endObject();
    }

    public static void ToXContent(StandardLocalFunction standardLocalFunction, UserTreePrinterScope scope) {
        start(standardLocalFunction, scope);
        scope.field("function");
        ToXContent(standardLocalFunction.getLocalFunction(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessClassBinding standardPainlessClassBinding, UserTreePrinterScope scope) {
        start(standardPainlessClassBinding, scope);
        scope.field("PainlessClassBinding");
        ToXContent(standardPainlessClassBinding.getPainlessClassBinding(), scope);
        scope.endObject();
    }

    public static void ToXContent(StandardPainlessInstanceBinding standardPainlessInstanceBinding, UserTreePrinterScope scope) {
        start(standardPainlessInstanceBinding, scope);
        scope.field("PainlessInstanceBinding");
        ToXContent(standardPainlessInstanceBinding.getPainlessInstanceBinding(), scope);
        scope.endObject();
    }

    public static void ToXContent(MethodNameDecoration methodNameDecoration, UserTreePrinterScope scope) {
        // TODO(stu): next
        start(methodNameDecoration, scope);

        scope.endObject();
    }

    public static void ToXContent(ReturnType returnType, UserTreePrinterScope scope) {
        start(returnType, scope);

        scope.endObject();
    }

    public static void ToXContent(TypeParameters typeParameters, UserTreePrinterScope scope) {
        start(typeParameters, scope);

        scope.endObject();
    }

    public static void ToXContent(ParameterNames parameterNames, UserTreePrinterScope scope) {
        start(parameterNames, scope);

        scope.endObject();
    }

    public static void ToXContent(ReferenceDecoration referenceDecoration, UserTreePrinterScope scope) {
        start(referenceDecoration, scope);

        scope.endObject();
    }

    public static void ToXContent(EncodingDecoration encodingDecoration, UserTreePrinterScope scope) {
        start(encodingDecoration, scope);

        scope.endObject();
    }

    public static void ToXContent(CapturesDecoration capturesDecoration, UserTreePrinterScope scope) {
        start(capturesDecoration, scope);

        scope.endObject();
    }

    public static void ToXContent(InstanceType instanceType, UserTreePrinterScope scope) {
        start(instanceType, scope);

        scope.endObject();
    }

    public static void ToXContent(AccessDepth accessDepth, UserTreePrinterScope scope) {
        start(accessDepth, scope);

        scope.endObject();
    }

    public static void ToXContent(IRNodeDecoration irNodeDecoration, UserTreePrinterScope scope) {
        start(irNodeDecoration, scope);

        scope.endObject();
    }

    public static void ToXContent(Converter Converter, UserTreePrinterScope scope) {
        start(Converter, scope);

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
        if (method.typeParameters != null && method.typeParameters.isEmpty() == false) {
            scope.field("typeParameters", classNames(method.typeParameters));
        }
        if (method.methodHandle != null) {
            scope.field("methodHandle");
            ToXContent(method.methodHandle.type(), scope);
        }
        // ignoring methodType as that's handled under methodHandle
        AnnotationsToXContent(method.annotations, scope);
        scope.endObject();
    }

    public static void ToXContent(FunctionTable.LocalFunction localFunction, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("functionName", localFunction.getFunctionName());
        scope.field("returnType", localFunction.getReturnType().getSimpleName());
        if (localFunction.getTypeParameters().isEmpty() == false) {
            scope.field("typeParameters", classNames(localFunction.getTypeParameters()));
        }
        scope.field("isInternal", localFunction.isInternal());
        scope.field("isStatic", localFunction.isStatic());
        scope.field("methodType");
        ToXContent(localFunction.getMethodType(), scope);
        scope.endObject();
    }

    // lookup
    public static void ToXContent(PainlessClassBinding binding, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("javaConstructor");
        ToXContent(binding.javaConstructor, scope);

        scope.field("javaMethod");
        ToXContent(binding.javaMethod, scope);
        scope.field("returnType", binding.returnType.getSimpleName());
        if (binding.typeParameters.isEmpty() == false) {
            scope.field("typeParameters", classNames(binding.typeParameters));
        }
        AnnotationsToXContent(binding.annotations, scope);
        scope.endObject();
    }

    public static void ToXContent(PainlessInstanceBinding binding, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("targetInstance", binding.targetInstance.getClass().getSimpleName());

        scope.field("javaMethod");
        ToXContent(binding.javaMethod, scope);
        scope.field("returnType", binding.returnType.getSimpleName());
        if (binding.typeParameters.isEmpty() == false) {
            scope.field("typeParameters", classNames(binding.typeParameters));
        }
        AnnotationsToXContent(binding.annotations, scope);
        scope.endObject();
    }

    public static void AnnotationsToXContent(Map<Class<?>, Object> annotations, UserTreePrinterScope scope) {
        if (annotations == null || annotations.isEmpty()) {
            return;
        }
        scope.startArray("annotations");
        for (Class<?> key : annotations.keySet().stream().sorted().collect(Collectors.toList())) {
            AnnotationToXContent(annotations.get(key), scope);
        }
        scope.endArray();
    }

    public static void AnnotationToXContent(Object annotation, UserTreePrinterScope scope) {
        if (annotation instanceof CompileTimeOnlyAnnotation) {
            scope.value(CompileTimeOnlyAnnotation.NAME);
        } else if (annotation instanceof DeprecatedAnnotation) {
            scope.startObject();
            scope.field("name", DeprecatedAnnotation.NAME);
            scope.field("message", ((DeprecatedAnnotation) annotation).getMessage());
            scope.endObject();
        } else if (annotation instanceof InjectConstantAnnotation) {
            scope.startObject();
            scope.field("name", InjectConstantAnnotation.NAME);
            scope.field("message", ((InjectConstantAnnotation) annotation).injects);
            scope.endObject();
        } else if (annotation instanceof NoImportAnnotation) {
            scope.value(NoImportAnnotation.NAME);
        } else if (annotation instanceof NonDeterministicAnnotation) {
            scope.value(NonDeterministicAnnotation.NAME);
        } else {
            scope.value(annotation.toString());
        }
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
        scope.field("javaConstructor");
        ToXContent(constructor.javaConstructor, scope);
        if (constructor.typeParameters.isEmpty() == false) {
            scope.field("typeParameters", classNames(constructor.typeParameters));
        }
        scope.field("methodHandle");
        ToXContent(constructor.methodHandle.type(), scope);
        scope.endObject();
    }

    public static void ToXContent(org.objectweb.asm.commons.Method asmMethod, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("name", asmMethod.getName());
        scope.field("descriptor", asmMethod.getDescriptor());
        scope.field("returnType", asmMethod.getReturnType().getClassName());
        scope.field("argumentTypes", Arrays.stream(asmMethod.getArgumentTypes()).map(Type::getClassName));
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

    public static void ToXContent(Constructor<?> constructor, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field("name", constructor.getName());
        if (constructor.getParameterTypes().length > 0) {
            scope.field("parameterTypes", classNames(constructor.getParameterTypes()));
        }
        if (constructor.getExceptionTypes().length > 0) {
            scope.field("exceptionTypes", classNames(constructor.getExceptionTypes()));
        }
        scope.field("modifiers", Modifier.toString(constructor.getModifiers()));
        scope.endObject();
    }

    // helpers
    public static void start(Decoration decoration, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.DECORATION, decoration.getClass().getSimpleName());
    }

    public static List<String> classNames(Class<?>[] classes) {
        return Arrays.stream(classes).map(Class::getSimpleName).collect(Collectors.toList());
    }

    public static List<String> classNames(List<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName).collect(Collectors.toList());
    }
}
