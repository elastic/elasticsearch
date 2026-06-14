/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.ArrayLength;
import org.elasticsearch.foreign.ArrayOf;
import org.elasticsearch.foreign.CaptureErrno;
import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.FunctionPointer;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Padding;
import org.elasticsearch.foreign.Setter;
import org.elasticsearch.foreign.Struct;
import org.elasticsearch.foreign.StructFactory;
import org.elasticsearch.foreign.SymbolResolverClass;
import org.elasticsearch.foreign.Utf16;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Models a {@code @LibrarySpecification}-annotated interface, built from the annotation processing
 * type model. Encapsulates all methods and nested structs needed for code generation.
 *
 * @param qualifiedName the fully-qualified interface name
 * @param simpleName the simple interface name
 * @param packageName the package name
 * @param libraryName the native library name from {@code @LibrarySpecification.name()} (may be empty)
 * @param methods all classified methods in declaration order
 * @param structs all nested {@code @Struct} interfaces in declaration order
 */
public record LibraryModel(
    String qualifiedName,
    String simpleName,
    String packageName,
    String libraryName,
    List<MethodModel> methods,
    List<StructModel> structs
) {

    /**
     * Builds a {@code LibraryModel} from a {@code @LibrarySpecification}-annotated interface element.
     * Emits {@link Kind#ERROR} diagnostics via the messager for any validation failure.
     *
     * @return the built model, or null if any error was emitted
     */
    public static LibraryModel from(TypeElement element, ProcessingEnvironment processingEnv) {
        Messager messager = processingEnv.getMessager();
        Elements elements = processingEnv.getElementUtils();
        Types types = processingEnv.getTypeUtils();
        boolean hasError = false;

        LibrarySpecification annotation = element.getAnnotation(LibrarySpecification.class);
        String libraryName = annotation != null ? annotation.name() : "";

        String qualifiedName = element.getQualifiedName().toString();
        String simpleName = element.getSimpleName().toString();
        String packageName = elements.getPackageOf(element).getQualifiedName().toString();

        List<MethodModel> methods = new ArrayList<>();
        List<StructModel> structs = new ArrayList<>();

        // Collect nested @Struct interfaces first so we can recognize them during method classification
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() == ElementKind.INTERFACE) {
                var nested = (TypeElement) enclosed;
                Struct struct = nested.getAnnotation(Struct.class);
                if (struct != null) {
                    StructModel structModel = buildStructModel(nested, struct, types, messager);
                    if (structModel == null) {
                        hasError = true;
                    } else {
                        structs.add(structModel);
                    }
                }
            }
        }

        // Classify each abstract interface method
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            var method = (ExecutableElement) enclosed;
            // Skip default and static methods
            if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }

            MethodModel methodModel = classifyMethod(method, structs, types, messager);
            if (methodModel == null) {
                hasError = true;
            } else {
                methods.add(methodModel);
            }
        }

        if (hasError) {
            return null;
        }
        return new LibraryModel(qualifiedName, simpleName, packageName, libraryName, methods, structs);
    }

    private static MethodModel classifyMethod(ExecutableElement method, List<StructModel> knownStructs, Types types, Messager messager) {
        String methodName = method.getSimpleName().toString();
        Function function = method.getAnnotation(Function.class);
        StructFactory structFactory = method.getAnnotation(StructFactory.class);
        Setter setter = method.getAnnotation(Setter.class);

        if (setter != null) {
            // @Setter is only valid inside @Struct interfaces, not on library methods
            messager.printMessage(
                Kind.ERROR,
                "@Setter annotation on method '"
                    + methodName
                    + "' is only valid inside a @Struct interface, not directly on a @LibrarySpecification interface",
                method
            );
            return null;
        }

        // Mutually exclusive: a method should have at most one of Function or StructFactory
        if (function != null && structFactory != null) {
            messager.printMessage(Kind.ERROR, "Method '" + methodName + "' cannot have both @Function and @StructFactory", method);
            return null;
        }

        if (function != null) {
            return classifyNativeFunctionMethod(method, function, types, messager);
        }

        if (structFactory != null) {
            return classifyStructFactoryMethod(method, knownStructs, messager);
        }

        messager.printMessage(
            Kind.ERROR,
            "Method '" + methodName + "' in @LibrarySpecification interface must be annotated with @Function or @StructFactory",
            method
        );
        return null;
    }

    private static MethodModel classifyNativeFunctionMethod(ExecutableElement method, Function function, Types types, Messager messager) {
        String methodName = method.getSimpleName().toString();
        String cSymbol = function.value();
        boolean isCritical = method.getAnnotation(Critical.class) != null;
        boolean capturesErrno = method.getAnnotation(CaptureErrno.class) != null;

        if (isCritical && capturesErrno) {
            messager.printMessage(
                Kind.ERROR,
                "Method '"
                    + methodName
                    + "' cannot combine @Critical and @CaptureErrno:"
                    + " critical calls bypass the errno-capture mechanism",
                method
            );
            return null;
        }

        // Resolve symbol resolver class name via annotation mirror (avoid MirroredTypeException)
        String symbolResolverClassName = null;
        SymbolResolverClass symbolResolverClass = method.getAnnotation(SymbolResolverClass.class);
        if (symbolResolverClass != null) {
            for (var mirror : method.getAnnotationMirrors()) {
                var annotationType = mirror.getAnnotationType().asElement();
                if (((TypeElement) annotationType).getQualifiedName().toString().equals(SymbolResolverClass.class.getName())) {
                    for (var entry : mirror.getElementValues().entrySet()) {
                        if (entry.getKey().getSimpleName().toString().equals("value")) {
                            symbolResolverClassName = entry.getValue().getValue().toString();
                        }
                    }
                }
            }
        }

        // Classify return type
        MethodModel.ReturnType returnType = classifyReturnType(method, messager);
        if (returnType == null) {
            return null;
        }

        // Check for @FunctionPointer parameter and Arena parameter
        boolean hasFunctionPointerParam = false;
        boolean hasArenaParam = false;
        String functionPointerTypeName = null;
        String functionPointerTypeFqn = null;
        String callbackMethodName = null;
        MethodModel.ReturnType callbackReturnType = null;
        List<MethodModel.ParamInfo> callbackParamTypes = null;
        int arenaParamIndex = -1;

        for (int i = 0; i < method.getParameters().size(); i++) {
            var param = method.getParameters().get(i);
            TypeElement paramType = asTypeElement(param.asType());
            if (paramType != null && paramType.getAnnotation(FunctionPointer.class) != null) {
                if (hasFunctionPointerParam) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Method '" + methodName + "' has multiple @FunctionPointer parameters; only one is allowed",
                        method
                    );
                    return null;
                }
                hasFunctionPointerParam = true;
                functionPointerTypeName = paramType.getSimpleName().toString();
                functionPointerTypeFqn = paramType.getQualifiedName().toString();
                ExecutableElement sam = findSamMethod(paramType, messager);
                if (sam == null) {
                    return null;
                }
                callbackMethodName = sam.getSimpleName().toString();
                callbackReturnType = classifyReturnType(sam, messager);
                if (callbackReturnType == null) {
                    return null;
                }
                callbackParamTypes = new ArrayList<>();
                for (var samParam : sam.getParameters()) {
                    MethodModel.ParamInfo info = classifyParam(samParam, messager);
                    if (info == null) {
                        return null;
                    }
                    callbackParamTypes.add(info);
                }
            }
            if (paramType != null && paramType.getQualifiedName().toString().equals("java.lang.foreign.Arena")) {
                if (hasArenaParam) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Method '" + methodName + "' has multiple Arena parameters; only one is allowed",
                        method
                    );
                    return null;
                }
                hasArenaParam = true;
                arenaParamIndex = i;
            }
        }

        if (hasFunctionPointerParam && !hasArenaParam) {
            messager.printMessage(
                Kind.ERROR,
                "Method '" + methodName + "' has a @FunctionPointer parameter but no Arena parameter declared",
                method
            );
            return null;
        }

        // Build param info list
        List<MethodModel.ParamInfo> paramInfos = new ArrayList<>();
        for (var param : method.getParameters()) {
            MethodModel.ParamInfo paramInfo = classifyParam(param, messager);
            if (paramInfo == null) {
                return null;
            }
            paramInfos.add(paramInfo);
        }

        if (hasFunctionPointerParam) {
            return new MethodModel.FunctionPointerMethod(
                methodName,
                cSymbol,
                returnType,
                functionPointerTypeName,
                functionPointerTypeFqn,
                callbackMethodName,
                callbackReturnType,
                callbackParamTypes,
                arenaParamIndex,
                paramInfos,
                isCritical,
                capturesErrno,
                symbolResolverClassName
            );
        }

        return new MethodModel.FunctionMethod(
            methodName,
            cSymbol,
            returnType,
            paramInfos,
            isCritical,
            capturesErrno,
            symbolResolverClassName
        );
    }

    private static MethodModel classifyStructFactoryMethod(ExecutableElement method, List<StructModel> knownStructs, Messager messager) {
        String methodName = method.getSimpleName().toString();
        TypeMirror returnType = method.getReturnType();

        if (returnType.getKind() != TypeKind.DECLARED) {
            messager.printMessage(Kind.ERROR, "@StructFactory method '" + methodName + "' must return a @Struct-annotated type", method);
            return null;
        }

        TypeElement returnElement = (TypeElement) ((DeclaredType) returnType).asElement();
        String structTypeName = returnElement.getSimpleName().toString();

        // Find the corresponding struct model
        StructModel structModel = knownStructs.stream().filter(s -> s.simpleName().equals(structTypeName)).findFirst().orElse(null);

        if (structModel == null) {
            messager.printMessage(
                Kind.ERROR,
                "@StructFactory method '" + methodName + "' returns '" + structTypeName + "' which is not a @Struct nested interface",
                method
            );
            return null;
        }

        var params = method.getParameters();

        // Check if it's an array factory: single parameter of element array type
        if (params.size() == 1 && params.get(0).asType().getKind() == TypeKind.ARRAY) {
            ArrayType arrayType = (ArrayType) params.get(0).asType();
            if (arrayType.getComponentType().getKind() == TypeKind.DECLARED) {
                TypeElement componentElement = (TypeElement) ((DeclaredType) arrayType.getComponentType()).asElement();
                return new MethodModel.ArrayFactoryMethod(methodName, structTypeName, componentElement.getSimpleName().toString());
            }
        }

        // Static or dynamic factory
        List<String> fieldNames = structModel.fields().stream().map(FieldModel::name).toList();
        return new MethodModel.StructFactoryMethod(methodName, structTypeName, structModel.isDynamic(), fieldNames);
    }

    private static MethodModel.ReturnType classifyReturnType(ExecutableElement method, Messager messager) {
        TypeMirror returnMirror = method.getReturnType();
        if (returnMirror.getKind() == TypeKind.VOID) {
            return MethodModel.ReturnType.VOID;
        }
        if (returnMirror.getKind() == TypeKind.DECLARED) {
            TypeElement returnElement = (TypeElement) ((DeclaredType) returnMirror).asElement();
            String returnFqn = returnElement.getQualifiedName().toString();
            if (returnFqn.equals("java.lang.String")) {
                boolean isUtf16 = method.getAnnotation(Utf16.class) != null;
                return MethodModel.ReturnType.ofString(isUtf16);
            }
            if (returnFqn.equals("java.lang.foreign.MemorySegment")) {
                return MethodModel.ReturnType.of(NativeType.ADDRESS);
            }
        }
        NativeType type = primitiveNativeType(returnMirror.getKind());
        if (type != null) {
            return MethodModel.ReturnType.of(type);
        }
        messager.printMessage(
            Kind.ERROR,
            "Unsupported return type '" + returnMirror + "' on method '" + method.getSimpleName() + "'",
            method
        );
        return null;
    }

    private static MethodModel.ParamInfo classifyParam(VariableElement param, Messager messager) {
        TypeMirror paramType = param.asType();
        boolean isUtf16 = param.getAnnotation(Utf16.class) != null;

        if (paramType.getKind() == TypeKind.DECLARED) {
            TypeElement paramElement = (TypeElement) ((DeclaredType) paramType).asElement();
            String fqn = paramElement.getQualifiedName().toString();

            if (fqn.equals("java.lang.String")) {
                return new MethodModel.ParamInfo(NativeType.STRING, isUtf16, null, null);
            }
            if (isUtf16) {
                messager.printMessage(Kind.ERROR, "@Utf16 on '" + param.getSimpleName() + "' is only valid on String parameters", param);
                return null;
            }
            if (fqn.equals("java.lang.foreign.MemorySegment")) {
                return MethodModel.ParamInfo.of(NativeType.ADDRESS);
            }
            if (fqn.equals("java.lang.foreign.Arena")) {
                // Arena is not a native argument — it provides lifetime for upcall stubs
                return MethodModel.ParamInfo.of(NativeType.ARENA);
            }
            if (paramElement.getAnnotation(FunctionPointer.class) != null) {
                return new MethodModel.ParamInfo(NativeType.ADDRESS, false, null, paramElement.getSimpleName().toString());
            }
            if (paramElement.getAnnotation(Struct.class) != null) {
                return new MethodModel.ParamInfo(NativeType.ADDRESS, false, paramElement.getSimpleName().toString(), null);
            }
        }

        NativeType type = primitiveNativeType(paramType.getKind());
        if (type != null) {
            if (isUtf16) {
                messager.printMessage(Kind.ERROR, "@Utf16 on '" + param.getSimpleName() + "' is only valid on String parameters", param);
                return null;
            }
            return MethodModel.ParamInfo.of(type);
        }

        messager.printMessage(
            Kind.ERROR,
            "Unsupported parameter type '" + paramType + "' on parameter '" + param.getSimpleName() + "'",
            param
        );
        return null;
    }

    private static StructModel buildStructModel(TypeElement structElement, Struct struct, Types types, Messager messager) {
        String simpleName = structElement.getSimpleName().toString();
        boolean isDynamic = struct.dynamicLayout();
        List<FieldModel> fields = new ArrayList<>();
        boolean hasError = false;

        ExecutableElement lastGetterMethod = null;
        FieldModel pendingField = null;

        for (var enclosed : structElement.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            var method = (ExecutableElement) enclosed;
            if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                // Flush pending field — this non-abstract method breaks setter adjacency
                if (pendingField != null) {
                    fields.add(pendingField);
                    pendingField = null;
                }
                lastGetterMethod = null;
                continue;
            }

            boolean isSetter = method.getAnnotation(Setter.class) != null;

            if (isSetter) {
                if (lastGetterMethod == null || pendingField == null) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@Setter method '" + method.getSimpleName() + "' must immediately follow a getter with the same name",
                        method
                    );
                    hasError = true;
                    lastGetterMethod = null;
                    pendingField = null;
                    continue;
                }
                String getterName = lastGetterMethod.getSimpleName().toString();
                String setterName = method.getSimpleName().toString();
                if (!getterName.equals(setterName)) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@Setter method '" + setterName + "' must have the same name as the preceding getter '" + getterName + "'",
                        method
                    );
                    hasError = true;
                    lastGetterMethod = null;
                    pendingField = null;
                    continue;
                }
                // Validate that the setter's parameter type matches the getter's return type
                TypeMirror getterReturn = lastGetterMethod.getReturnType();
                if (method.getParameters().size() != 1 || !types.isSameType(method.getParameters().get(0).asType(), getterReturn)) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@Setter method '" + setterName + "' parameter type must match the getter return type '" + getterReturn + "'",
                        method
                    );
                    hasError = true;
                    lastGetterMethod = null;
                    pendingField = null;
                    continue;
                }
                // Promote pending field to have a setter
                pendingField = new FieldModel(
                    pendingField.name(),
                    pendingField.nativeType(),
                    pendingField.paddingBefore(),
                    pendingField.arrayOf(),
                    pendingField.arrayLengthFor(),
                    true
                );
                fields.add(pendingField);
                lastGetterMethod = null;
                pendingField = null;
                continue;
            }

            // A getter: zero params, non-void return
            if (!method.getParameters().isEmpty() || method.getReturnType().getKind() == TypeKind.VOID) {
                messager.printMessage(
                    Kind.ERROR,
                    "Method '"
                        + method.getSimpleName()
                        + "' in @Struct '"
                        + simpleName
                        + "' is not a getter (must have no params and non-void return) "
                        + "and is not annotated with @Setter",
                    method
                );
                hasError = true;
                lastGetterMethod = null;
                pendingField = null;
                continue;
            }

            // Flush any pending field without setter before processing the next getter
            if (pendingField != null) {
                fields.add(pendingField);
            }

            String fieldName = method.getSimpleName().toString();
            NativeType type = classifyStructFieldType(method.getReturnType(), messager, method);
            if (type == null) {
                hasError = true;
                lastGetterMethod = null;
                pendingField = null;
                continue;
            }

            int paddingBefore = 0;
            Padding padding = method.getAnnotation(Padding.class);
            if (padding != null) {
                paddingBefore = padding.bytes();
            }

            String arrayOf = null;
            ArrayOf arrayOfAnnotation = method.getAnnotation(ArrayOf.class);
            if (arrayOfAnnotation != null) {
                // MirroredTypeException-safe: get the type name via annotation mirror
                for (var mirror : method.getAnnotationMirrors()) {
                    var annotationType = mirror.getAnnotationType().asElement();
                    if (((TypeElement) annotationType).getQualifiedName().toString().equals(ArrayOf.class.getName())) {
                        for (var entry : mirror.getElementValues().entrySet()) {
                            if (entry.getKey().getSimpleName().toString().equals("value")) {
                                arrayOf = entry.getValue().getValue().toString();
                            }
                        }
                    }
                }
            }

            String arrayLengthFor = null;
            ArrayLength arrayLength = method.getAnnotation(ArrayLength.class);
            if (arrayLength != null) {
                arrayLengthFor = arrayLength.value();
            }

            pendingField = new FieldModel(fieldName, type, paddingBefore, arrayOf, arrayLengthFor, false);
            lastGetterMethod = method;
        }

        // Flush the last pending field (no following setter)
        if (pendingField != null) {
            fields.add(pendingField);
        }

        if (hasError) {
            return null;
        }
        return new StructModel(simpleName, isDynamic, fields);
    }

    private static NativeType classifyStructFieldType(TypeMirror type, Messager messager, javax.lang.model.element.Element context) {
        if (type.getKind() == TypeKind.DECLARED) {
            TypeElement element = (TypeElement) ((DeclaredType) type).asElement();
            String fqn = element.getQualifiedName().toString();
            if (fqn.equals("java.lang.String")) {
                return NativeType.STRING;
            }
            if (fqn.equals("java.lang.foreign.MemorySegment")) {
                return NativeType.ADDRESS;
            }
            if (element.getAnnotation(Struct.class) != null || element.getAnnotation(FunctionPointer.class) != null) {
                return NativeType.ADDRESS;
            }
        }
        NativeType nativeType = primitiveNativeType(type.getKind());
        if (nativeType != null) {
            return nativeType;
        }
        messager.printMessage(Kind.ERROR, "Unsupported struct field type '" + type + "'", context);
        return null;
    }

    private static NativeType primitiveNativeType(TypeKind typeKind) {
        return switch (typeKind) {
            case INT -> NativeType.INT;
            case LONG -> NativeType.LONG;
            case SHORT -> NativeType.SHORT;
            case BYTE -> NativeType.BYTE;
            case BOOLEAN -> NativeType.BOOLEAN;
            case FLOAT -> NativeType.FLOAT;
            case DOUBLE -> NativeType.DOUBLE;
            default -> null;
        };
    }

    /**
     * Finds the single abstract method of a {@code @FunctionPointer}-annotated interface.
     * Emits an error and returns null if the interface has any number of abstract methods other than one.
     */
    private static ExecutableElement findSamMethod(TypeElement callbackType, Messager messager) {
        List<ExecutableElement> abstractMethods = new ArrayList<>();
        for (var e : callbackType.getEnclosedElements()) {
            if (e.getKind() != ElementKind.METHOD) {
                continue;
            }
            var m = (ExecutableElement) e;
            if (m.getModifiers().contains(Modifier.DEFAULT) == false && m.getModifiers().contains(Modifier.STATIC) == false) {
                abstractMethods.add(m);
            }
        }
        if (abstractMethods.size() != 1) {
            messager.printMessage(
                Kind.ERROR,
                "@FunctionPointer type '"
                    + callbackType.getSimpleName()
                    + "' must have exactly one abstract method, found "
                    + abstractMethods.size(),
                callbackType
            );
            return null;
        }
        return abstractMethods.get(0);
    }

    private static TypeElement asTypeElement(TypeMirror mirror) {
        if (mirror.getKind() != TypeKind.DECLARED) {
            return null;
        }
        return (TypeElement) ((DeclaredType) mirror).asElement();
    }
}
