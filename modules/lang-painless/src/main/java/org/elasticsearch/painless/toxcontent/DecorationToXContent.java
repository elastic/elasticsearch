/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.toxcontent;

import org.elasticsearch.painless.FunctionRef;
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

import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialize user tree decorations from org.elasticsearch.painless.symbol.Decorations
 */
public class DecorationToXContent {
    static final class Fields {
        static final String DECORATION = "decoration";
        static final String TYPE = "type";
        static final String CAST = "cast";
        static final String METHOD = "method";
    }

    public static void ToXContent(TargetType targetType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(targetType, builder);
        builder.field(Fields.TYPE, targetType.getTargetType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(ValueType valueType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(valueType, builder);
        builder.field(Fields.TYPE, valueType.getValueType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(StaticType staticType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(staticType, builder);
        builder.field(Fields.TYPE, staticType.getStaticType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(PartialCanonicalTypeName partialCanonicalTypeName, XContentBuilderUncheckedExceptionWrapper builder) {
        start(partialCanonicalTypeName, builder);
        builder.field(Fields.TYPE, partialCanonicalTypeName.getPartialCanonicalTypeName());
        builder.endObject();
    }

    public static void ToXContent(ExpressionPainlessCast expressionPainlessCast, XContentBuilderUncheckedExceptionWrapper builder) {
        start(expressionPainlessCast, builder);
        builder.field(Fields.CAST);
        ToXContent(expressionPainlessCast.getExpressionPainlessCast(), builder);
        builder.endObject();
    }

    public static void ToXContent(SemanticVariable semanticVariable, XContentBuilderUncheckedExceptionWrapper builder) {
        start(semanticVariable, builder);
        builder.field("variable");
        ToXContent(semanticVariable.getSemanticVariable(), builder);
        builder.endObject();
    }

    public static void ToXContent(IterablePainlessMethod iterablePainlessMethod, XContentBuilderUncheckedExceptionWrapper builder) {
        start(iterablePainlessMethod, builder);
        builder.field(Fields.METHOD);
        ToXContent(iterablePainlessMethod.getIterablePainlessMethod(), builder);
        builder.endObject();
    }

    public static void ToXContent(UnaryType unaryType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(unaryType, builder);
        builder.field(Fields.TYPE, unaryType.getUnaryType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(BinaryType binaryType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(binaryType, builder);
        builder.field(Fields.TYPE, binaryType.getBinaryType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(ShiftType shiftType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(shiftType, builder);
        builder.field(Fields.TYPE, shiftType.getShiftType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(ComparisonType comparisonType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(comparisonType, builder);
        builder.field(Fields.TYPE, comparisonType.getComparisonType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(CompoundType compoundType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(compoundType, builder);
        builder.field(Fields.TYPE, compoundType.getCompoundType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(UpcastPainlessCast upcastPainlessCast, XContentBuilderUncheckedExceptionWrapper builder) {
        start(upcastPainlessCast, builder);
        builder.field(Fields.CAST);
        ToXContent(upcastPainlessCast.getUpcastPainlessCast(), builder);
        builder.endObject();
    }

    public static void ToXContent(DowncastPainlessCast downcastPainlessCast, XContentBuilderUncheckedExceptionWrapper builder) {
        start(downcastPainlessCast, builder);
        builder.field(Fields.CAST);
        ToXContent(downcastPainlessCast.getDowncastPainlessCast(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardPainlessField standardPainlessField, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardPainlessField, builder);
        builder.field("field");
        ToXContent(standardPainlessField.getStandardPainlessField(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardPainlessConstructor standardPainlessConstructor, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardPainlessConstructor, builder);
        builder.field("constructor");
        ToXContent(standardPainlessConstructor.getStandardPainlessConstructor(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardPainlessMethod standardPainlessMethod, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardPainlessMethod, builder);
        builder.field(Fields.METHOD);
        ToXContent(standardPainlessMethod.getStandardPainlessMethod(), builder);
        builder.endObject();
    }

    public static void ToXContent(GetterPainlessMethod getterPainlessMethod, XContentBuilderUncheckedExceptionWrapper builder) {
        start(getterPainlessMethod, builder);
        builder.field(Fields.METHOD);
        ToXContent(getterPainlessMethod.getGetterPainlessMethod(), builder);
        builder.endObject();
    }

    public static void ToXContent(SetterPainlessMethod setterPainlessMethod, XContentBuilderUncheckedExceptionWrapper builder) {
        start(setterPainlessMethod, builder);
        builder.field(Fields.METHOD);
        ToXContent(setterPainlessMethod.getSetterPainlessMethod(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardConstant standardConstant, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardConstant, builder);
        builder.startObject("constant");
        builder.field(Fields.TYPE, standardConstant.getStandardConstant().getClass().getSimpleName());
        builder.field("value", standardConstant.getStandardConstant());
        builder.endObject();
        builder.endObject();
    }

    public static void ToXContent(StandardLocalFunction standardLocalFunction, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardLocalFunction, builder);
        builder.field("function");
        ToXContent(standardLocalFunction.getLocalFunction(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardPainlessClassBinding standardPainlessClassBinding, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardPainlessClassBinding, builder);
        builder.field("PainlessClassBinding");
        ToXContent(standardPainlessClassBinding.getPainlessClassBinding(), builder);
        builder.endObject();
    }

    public static void ToXContent(StandardPainlessInstanceBinding standardPainlessInstanceBinding, XContentBuilderUncheckedExceptionWrapper builder) {
        start(standardPainlessInstanceBinding, builder);
        builder.field("PainlessInstanceBinding");
        ToXContent(standardPainlessInstanceBinding.getPainlessInstanceBinding(), builder);
        builder.endObject();
    }

    public static void ToXContent(MethodNameDecoration methodNameDecoration, XContentBuilderUncheckedExceptionWrapper builder) {
        start(methodNameDecoration, builder);
        builder.field("methodName", methodNameDecoration.getMethodName());
        builder.endObject();
    }

    public static void ToXContent(ReturnType returnType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(returnType, builder);
        builder.field("returnType", returnType.getReturnType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(TypeParameters typeParameters, XContentBuilderUncheckedExceptionWrapper builder) {
        start(typeParameters, builder);
        if (typeParameters.getTypeParameters().isEmpty() == false) {
            builder.field("typeParameters", classNames(typeParameters.getTypeParameters()));
        }
        builder.endObject();
    }

    public static void ToXContent(ParameterNames parameterNames, XContentBuilderUncheckedExceptionWrapper builder) {
        start(parameterNames, builder);
        if (parameterNames.getParameterNames().isEmpty() == false) {
            builder.field("parameterNames", parameterNames.getParameterNames());
        }
        builder.endObject();
    }

    public static void ToXContent(ReferenceDecoration referenceDecoration, XContentBuilderUncheckedExceptionWrapper builder) {
        start(referenceDecoration, builder);
        FunctionRef ref = referenceDecoration.getReference();
        builder.field("interfaceMethodName", ref.interfaceMethodName);

        builder.field("interfaceMethodType");
        ToXContent(ref.interfaceMethodType, builder);

        builder.field("delegateClassName", ref.delegateClassName);
        builder.field("isDelegateInterface", ref.isDelegateInterface);
        builder.field("isDelegateAugmented", ref.isDelegateAugmented);
        builder.field("delegateInvokeType", ref.delegateInvokeType);
        builder.field("delegateMethodName", ref.delegateMethodName);

        builder.field("delegateMethodType");
        ToXContent(ref.delegateMethodType, builder);

        if (ref.delegateInjections.length > 0) {
            builder.startArray("delegateInjections");
            for (Object obj : ref.delegateInjections) {
                builder.startObject();
                builder.field("type", obj.getClass().getSimpleName());
                builder.field("value", obj);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.field("factoryMethodType");
        ToXContent(ref.factoryMethodType, builder);
        builder.endObject();
    }

    public static void ToXContent(EncodingDecoration encodingDecoration, XContentBuilderUncheckedExceptionWrapper builder) {
        start(encodingDecoration, builder);
        builder.field("encoding", encodingDecoration.getEncoding());
        builder.endObject();
    }

    public static void ToXContent(CapturesDecoration capturesDecoration, XContentBuilderUncheckedExceptionWrapper builder) {
        start(capturesDecoration, builder);
        if (capturesDecoration.getCaptures().isEmpty() == false) {
            builder.startArray("captures");
            for (SemanticScope.Variable capture : capturesDecoration.getCaptures()) {
                ToXContent(capture, builder);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static void ToXContent(InstanceType instanceType, XContentBuilderUncheckedExceptionWrapper builder) {
        start(instanceType, builder);
        builder.field("instanceType", instanceType.getInstanceType().getSimpleName());
        builder.endObject();
    }

    public static void ToXContent(AccessDepth accessDepth, XContentBuilderUncheckedExceptionWrapper builder) {
        start(accessDepth, builder);
        builder.field("depth", accessDepth.getAccessDepth());
        builder.endObject();
    }

    public static void ToXContent(IRNodeDecoration irNodeDecoration, XContentBuilderUncheckedExceptionWrapper builder) {
        start(irNodeDecoration, builder);
        // TODO(stu): expand this
        builder.field("irNode", irNodeDecoration.getIRNode().toString());
        builder.endObject();
    }

    public static void ToXContent(Converter converter, XContentBuilderUncheckedExceptionWrapper builder) {
        start(converter, builder);
        builder.field("converter");
        ToXContent(converter.getConverter(), builder);
        builder.endObject();
    }

    public static void ToXContent(Decoration decoration, XContentBuilderUncheckedExceptionWrapper builder) {
        if  (decoration instanceof TargetType) {
            ToXContent((TargetType) decoration, builder);
        } else if (decoration instanceof ValueType) {
            ToXContent((ValueType) decoration, builder);
        } else if (decoration instanceof StaticType) {
            ToXContent((StaticType) decoration, builder);
        } else if (decoration instanceof PartialCanonicalTypeName) {
            ToXContent((PartialCanonicalTypeName) decoration, builder);
        } else if (decoration instanceof ExpressionPainlessCast) {
            ToXContent((ExpressionPainlessCast) decoration, builder);
        } else if (decoration instanceof SemanticVariable) {
            ToXContent((SemanticVariable) decoration, builder);
        } else if (decoration instanceof IterablePainlessMethod) {
            ToXContent((IterablePainlessMethod) decoration, builder);
        } else if (decoration instanceof UnaryType) {
            ToXContent((UnaryType) decoration, builder);
        } else if (decoration instanceof BinaryType) {
            ToXContent((BinaryType) decoration, builder);
        } else if (decoration instanceof ShiftType) {
            ToXContent((ShiftType) decoration, builder);
        } else if (decoration instanceof ComparisonType) {
            ToXContent((ComparisonType) decoration, builder);
        } else if (decoration instanceof CompoundType) {
            ToXContent((CompoundType) decoration, builder);
        } else if (decoration instanceof UpcastPainlessCast) {
            ToXContent((UpcastPainlessCast) decoration, builder);
        } else if (decoration instanceof DowncastPainlessCast) {
            ToXContent((DowncastPainlessCast) decoration, builder);
        } else if (decoration instanceof StandardPainlessField) {
            ToXContent((StandardPainlessField) decoration, builder);
        } else if (decoration instanceof StandardPainlessConstructor) {
            ToXContent((StandardPainlessConstructor) decoration, builder);
        } else if (decoration instanceof StandardPainlessMethod) {
            ToXContent((StandardPainlessMethod) decoration, builder);
        } else if (decoration instanceof GetterPainlessMethod) {
            ToXContent((GetterPainlessMethod) decoration, builder);
        } else if (decoration instanceof SetterPainlessMethod) {
            ToXContent((SetterPainlessMethod) decoration, builder);
        } else if (decoration instanceof StandardConstant) {
            ToXContent((StandardConstant) decoration, builder);
        } else if (decoration instanceof StandardLocalFunction) {
            ToXContent((StandardLocalFunction) decoration, builder);
        } else if (decoration instanceof StandardPainlessClassBinding) {
            ToXContent((StandardPainlessClassBinding) decoration, builder);
        } else if (decoration instanceof StandardPainlessInstanceBinding) {
            ToXContent((StandardPainlessInstanceBinding) decoration, builder);
        } else if (decoration instanceof MethodNameDecoration) {
            ToXContent((MethodNameDecoration) decoration, builder);
        } else if (decoration instanceof ReturnType) {
            ToXContent((ReturnType) decoration, builder);
        } else if (decoration instanceof TypeParameters) {
            ToXContent((TypeParameters) decoration, builder);
        } else if (decoration instanceof ParameterNames) {
            ToXContent((ParameterNames) decoration, builder);
        } else if (decoration instanceof ReferenceDecoration) {
            ToXContent((ReferenceDecoration) decoration, builder);
        } else if (decoration instanceof EncodingDecoration) {
            ToXContent((EncodingDecoration) decoration, builder);
        } else if (decoration instanceof CapturesDecoration) {
            ToXContent((CapturesDecoration) decoration, builder);
        } else if (decoration instanceof InstanceType) {
            ToXContent((InstanceType) decoration, builder);
        } else if (decoration instanceof AccessDepth) {
            ToXContent((AccessDepth) decoration, builder);
        } else if (decoration instanceof IRNodeDecoration) {
            ToXContent((IRNodeDecoration) decoration, builder);
        } else if (decoration instanceof Converter) {
            ToXContent((Converter) decoration, builder);
        } else {
            builder.startObject();
            builder.field(Fields.DECORATION, decoration.getClass().getSimpleName());
            builder.endObject();
        }
    }

    // lookup
    public static void ToXContent(PainlessCast painlessCast, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        if (painlessCast.originalType != null) {
            builder.field("originalType", painlessCast.originalType.getSimpleName());
        }
        if (painlessCast.targetType != null) {
            builder.field("targetType", painlessCast.targetType.getSimpleName());
        }

        builder.field("explicitCast", painlessCast.explicitCast);

        if (painlessCast.unboxOriginalType != null) {
            builder.field("unboxOriginalType", painlessCast.unboxOriginalType.getSimpleName());
        }
        if (painlessCast.unboxTargetType != null) {
            builder.field("unboxTargetType", painlessCast.unboxTargetType.getSimpleName());
        }
        if (painlessCast.boxOriginalType != null) {
            builder.field("boxOriginalType", painlessCast.boxOriginalType.getSimpleName());
        }
        builder.endObject();
    }

    public static void ToXContent(PainlessMethod method, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        if (method.javaMethod != null) {
            builder.field("javaMethod");
            ToXContent(method.methodType, builder);
        }
        if (method.targetClass != null) {
            builder.field("targetClass", method.targetClass.getSimpleName());
        }
        if (method.returnType != null) {
            builder.field("returnType", method.returnType.getSimpleName());
        }
        if (method.typeParameters != null && method.typeParameters.isEmpty() == false) {
            builder.field("typeParameters", classNames(method.typeParameters));
        }
        if (method.methodHandle != null) {
            builder.field("methodHandle");
            ToXContent(method.methodHandle.type(), builder);
        }
        // ignoring methodType as that's handled under methodHandle
        AnnotationsToXContent(method.annotations, builder);
        builder.endObject();
    }

    public static void ToXContent(FunctionTable.LocalFunction localFunction, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("functionName", localFunction.getFunctionName());
        builder.field("returnType", localFunction.getReturnType().getSimpleName());
        if (localFunction.getTypeParameters().isEmpty() == false) {
            builder.field("typeParameters", classNames(localFunction.getTypeParameters()));
        }
        builder.field("isInternal", localFunction.isInternal());
        builder.field("isStatic", localFunction.isStatic());
        builder.field("methodType");
        ToXContent(localFunction.getMethodType(), builder);
        builder.endObject();
    }

    public static void ToXContent(PainlessClassBinding binding, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("javaConstructor");
        ToXContent(binding.javaConstructor, builder);

        builder.field("javaMethod");
        ToXContent(binding.javaMethod, builder);
        builder.field("returnType", binding.returnType.getSimpleName());
        if (binding.typeParameters.isEmpty() == false) {
            builder.field("typeParameters", classNames(binding.typeParameters));
        }
        AnnotationsToXContent(binding.annotations, builder);
        builder.endObject();
    }

    public static void ToXContent(PainlessInstanceBinding binding, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("targetInstance", binding.targetInstance.getClass().getSimpleName());

        builder.field("javaMethod");
        ToXContent(binding.javaMethod, builder);
        builder.field("returnType", binding.returnType.getSimpleName());
        if (binding.typeParameters.isEmpty() == false) {
            builder.field("typeParameters", classNames(binding.typeParameters));
        }
        AnnotationsToXContent(binding.annotations, builder);
        builder.endObject();
    }

    public static void ToXContent(PainlessField field, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("javaField");
        ToXContent(field.javaField, builder);
        builder.field("typeParameter", field.typeParameter.getSimpleName());
        builder.field("getterMethodHandle");
        ToXContent(field.getterMethodHandle.type(), builder);
        builder.field("setterMethodHandle");
        if (field.setterMethodHandle != null) {
            ToXContent(field.setterMethodHandle.type(), builder);
        }
        builder.endObject();
    }

    public static void ToXContent(PainlessConstructor constructor, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("javaConstructor");
        ToXContent(constructor.javaConstructor, builder);
        if (constructor.typeParameters.isEmpty() == false) {
            builder.field("typeParameters", classNames(constructor.typeParameters));
        }
        builder.field("methodHandle");
        ToXContent(constructor.methodHandle.type(), builder);
        builder.endObject();
    }

    // symbol
    public static void ToXContent(SemanticScope.Variable variable, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field(Fields.TYPE, variable.getType());
        builder.field("name", variable.getName());
        builder.field("isFinal", variable.isFinal());
        builder.endObject();
    }

    // annotations
    public static void AnnotationsToXContent(Map<Class<?>, Object> annotations, XContentBuilderUncheckedExceptionWrapper builder) {
        if (annotations == null || annotations.isEmpty()) {
            return;
        }
        builder.startArray("annotations");
        for (Class<?> key : annotations.keySet().stream().sorted().collect(Collectors.toList())) {
            AnnotationToXContent(annotations.get(key), builder);
        }
        builder.endArray();
    }

    public static void AnnotationToXContent(Object annotation, XContentBuilderUncheckedExceptionWrapper builder) {
        if (annotation instanceof CompileTimeOnlyAnnotation) {
            builder.value(CompileTimeOnlyAnnotation.NAME);
        } else if (annotation instanceof DeprecatedAnnotation) {
            builder.startObject();
            builder.field("name", DeprecatedAnnotation.NAME);
            builder.field("message", ((DeprecatedAnnotation) annotation).getMessage());
            builder.endObject();
        } else if (annotation instanceof InjectConstantAnnotation) {
            builder.startObject();
            builder.field("name", InjectConstantAnnotation.NAME);
            builder.field("message", ((InjectConstantAnnotation) annotation).injects);
            builder.endObject();
        } else if (annotation instanceof NoImportAnnotation) {
            builder.value(NoImportAnnotation.NAME);
        } else if (annotation instanceof NonDeterministicAnnotation) {
            builder.value(NonDeterministicAnnotation.NAME);
        } else {
            builder.value(annotation.toString());
        }
    }

    // asm
    public static void ToXContent(org.objectweb.asm.commons.Method asmMethod, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("name", asmMethod.getName());
        builder.field("descriptor", asmMethod.getDescriptor());
        builder.field("returnType", asmMethod.getReturnType().getClassName());
        builder.field("argumentTypes", Arrays.stream(asmMethod.getArgumentTypes()).map(Type::getClassName));
        builder.endObject();
    }

    // java.lang.invoke
    public static void ToXContent(MethodType methodType, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        List<Class<?>> parameters = methodType.parameterList();
        if (parameters.isEmpty() == false) {
            builder.field("parameters", classNames(parameters));
        }
        builder.field("return", methodType.returnType().getSimpleName());
        builder.endObject();
    }

    // java.lang.reflect
    public static void ToXContent(Field field, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("name", field.getName());
        builder.field("type", field.getType().getSimpleName());
        builder.field("modifiers", Modifier.toString(field.getModifiers()));
        builder.endObject();
    }

    public static void ToXContent(Method method, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("name", method.getName());
        builder.field("parameters", classNames(method.getParameterTypes()));
        builder.field("return", method.getReturnType().getSimpleName());
        Class<?>[] exceptions = method.getExceptionTypes();
        if (exceptions.length > 0) {
            builder.field("exceptions", classNames(exceptions));
        }
        builder.field("modifiers", Modifier.toString(method.getModifiers()));
        builder.endObject();
    }

    public static void ToXContent(Constructor<?> constructor, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field("name", constructor.getName());
        if (constructor.getParameterTypes().length > 0) {
            builder.field("parameterTypes", classNames(constructor.getParameterTypes()));
        }
        if (constructor.getExceptionTypes().length > 0) {
            builder.field("exceptionTypes", classNames(constructor.getExceptionTypes()));
        }
        builder.field("modifiers", Modifier.toString(constructor.getModifiers()));
        builder.endObject();
    }

    // helpers
    public static void start(Decoration decoration, XContentBuilderUncheckedExceptionWrapper builder) {
        builder.startObject();
        builder.field(Fields.DECORATION, decoration.getClass().getSimpleName());
    }

    public static List<String> classNames(Class<?>[] classes) {
        return Arrays.stream(classes).map(Class::getSimpleName).collect(Collectors.toList());
    }

    public static List<String> classNames(List<Class<?>> classes) {
        return classes.stream().map(Class::getSimpleName).collect(Collectors.toList());
    }
}
