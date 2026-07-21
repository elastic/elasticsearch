/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic.Kind;

/**
 * Parses {@code @StructSpecification}-annotated types into {@link StructModel} instances.
 * All annotation-processing API usage for struct building is concentrated here, keeping
 * the model types themselves as plain data records.
 */
class StructSpecParser {

    public static final String ARRAY_FIELD_FQN = org.elasticsearch.foreign.ArrayField.class.getName();
    private static final String ADDRESSABLE_FQN = org.elasticsearch.foreign.Addressable.class.getName();

    /**
     * Builds a {@link StructModel} for a {@code @StructSpecification} record. Emits errors for any
     * unsupported record component types and returns {@code null} if any error was emitted.
     */
    static StructModel fromRecord(TypeElement typeElement, Messager messager) {
        String typeSimpleName = typeElement.getSimpleName().toString();
        List<StructFieldModel> fields = new ArrayList<>();
        boolean fieldError = false;
        for (RecordComponentElement component : typeElement.getRecordComponents()) {
            NativeType fieldType = ModelUtil.classifyType(component.asType());
            if (fieldType == null
                || fieldType == NativeType.VOID
                || fieldType == NativeType.STRING
                || fieldType == NativeType.ADDRESSABLE) {
                messager.printMessage(
                    Kind.ERROR,
                    "Unsupported field type '"
                        + component.asType()
                        + "' on component '"
                        + component.getSimpleName()
                        + "' of @StructSpecification record '"
                        + typeSimpleName
                        + "'",
                    component
                );
                fieldError = true;
            } else {
                fields.add(new ScalarFieldModel(component.getSimpleName().toString(), fieldType, true, false));
            }
        }
        return fieldError ? null : new StructRecordModel(typeSimpleName, List.copyOf(fields));
    }

    /**
     * Builds a {@link StructModel} for a {@code @StructSpecification} interface. Collects a
     * {@link StructFieldModel} for every abstract method (scalar or {@code @ArrayField}), merging
     * getter/setter pairs with the same name into a single {@link ScalarFieldModel}, and validates
     * that every {@code @ArrayField}'s {@code lengthField} references a real scalar field on the
     * same struct. Returns {@code null} on any error.
     */
    static StructModel fromInterface(TypeElement typeElement, List<String> priorStructNames, ProcessingEnvironment env, Messager messager) {
        String typeSimpleName = typeElement.getSimpleName().toString();

        // Raw list of field models before merging getter/setter pairs
        List<StructFieldModel> rawFields = new ArrayList<>();
        boolean fieldError = false;
        for (var enclosedMember : typeElement.getEnclosedElements()) {
            if (enclosedMember.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosedMember;
            var mods = method.getModifiers();
            if (mods.contains(Modifier.DEFAULT) || mods.contains(Modifier.STATIC)) {
                continue;
            }
            StructFieldModel fieldModel = buildInterfaceStructField(method, typeSimpleName, priorStructNames, env, messager);
            if (fieldModel == null) {
                fieldError = true;
                continue;
            }
            rawFields.add(fieldModel);
        }

        // Merge getter/setter pairs: require that the setter immediately follows the getter (or vice
        // versa) in the interface declaration — this enforces a predictable struct field order.
        List<StructFieldModel> interfaceFields = new ArrayList<>();
        List<String> scalarFieldNames = new ArrayList<>();
        for (StructFieldModel rawField : rawFields) {
            StructFieldModel last = interfaceFields.isEmpty() ? null : interfaceFields.getLast();
            if (last != null && last.name().equals(rawField.name())) {
                // Adjacent pair with the same name: attempt to merge getter and setter
                StructFieldModel merged = mergeAdjacentFields(last, rawField, typeSimpleName, typeElement, messager);
                if (merged == null) {
                    fieldError = true;
                } else {
                    interfaceFields.set(interfaceFields.size() - 1, merged);
                }
            } else if (interfaceFields.stream().anyMatch(f -> f.name().equals(rawField.name()))) {
                // A field with this name exists but is not adjacent: ordering would break the layout
                messager.printMessage(
                    Kind.ERROR,
                    "getter and setter for '" + rawField.name() + "' on '" + typeSimpleName + "' must be declared adjacent",
                    typeElement
                );
                fieldError = true;
            } else {
                // New field: add it as-is
                interfaceFields.add(rawField);
                if (rawField instanceof ScalarFieldModel scalar) {
                    scalarFieldNames.add(scalar.name());
                }
            }
        }

        // Every @ArrayField's lengthField must name a real scalar field on this same struct.
        for (StructFieldModel fm : interfaceFields) {
            if (fm instanceof ArrayFieldModel array && scalarFieldNames.contains(array.lengthFieldName()) == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField on '"
                        + array.name()
                        + "' references lengthField '"
                        + array.lengthFieldName()
                        + "' which is not a scalar field on '"
                        + typeSimpleName
                        + "'",
                    typeElement
                );
                fieldError = true;
            }
        }

        return fieldError ? null : new StructInterfaceModel(typeSimpleName, List.copyOf(interfaceFields));
    }

    /**
     * Merges two adjacent field models with the same name into a single getter+setter model.
     * The two models must be of the same concrete type; mixing annotation types on adjacent methods
     * with the same name is an error. Returns {@code null} (with an error already emitted) if any
     * validation constraint is violated.
     */
    private static StructFieldModel mergeAdjacentFields(
        StructFieldModel existing,
        StructFieldModel incoming,
        String structName,
        TypeElement typeElement,
        Messager messager
    ) {
        if (existing.getClass() != incoming.getClass()) {
            messager.printMessage(
                Kind.ERROR,
                "Field '" + incoming.name() + "' on '" + structName + "' has adjacent methods with different annotation types",
                typeElement
            );
            return null;
        }
        return switch (existing) {
            case ScalarFieldModel e -> {
                ScalarFieldModel i = (ScalarFieldModel) incoming;
                if (e.hasSetter() && i.hasSetter()) {
                    messager.printMessage(Kind.ERROR, "Duplicate setter for field '" + e.name() + "' on '" + structName + "'", typeElement);
                    yield null;
                }
                if (e.type() != i.type()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Getter and setter for field '"
                            + e.name()
                            + "' on '"
                            + structName
                            + "' have mismatched types: "
                            + (e.hasGetter() ? "getter" : "setter")
                            + " has '"
                            + e.type()
                            + "', "
                            + (i.hasGetter() ? "getter" : "setter")
                            + " has '"
                            + i.type()
                            + "'",
                        typeElement
                    );
                    yield null;
                }
                yield new ScalarFieldModel(e.name(), e.type(), e.hasGetter() || i.hasGetter(), e.hasSetter() || i.hasSetter());
            }
            case InlineArrayFieldModel e -> {
                InlineArrayFieldModel i = (InlineArrayFieldModel) incoming;
                if (e.hasSetter() && i.hasSetter()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate @InlineArrayField setter for field '" + e.name() + "' on '" + structName + "'",
                        typeElement
                    );
                    yield null;
                }
                if (e.elementType() != i.elementType()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineArrayField getter and setter for field '"
                            + e.name()
                            + "' on '"
                            + structName
                            + "' have mismatched element types: '"
                            + e.elementType()
                            + "' vs '"
                            + i.elementType()
                            + "'",
                        typeElement
                    );
                    yield null;
                }
                if (e.length() != i.length()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineArrayField getter and setter for field '"
                            + e.name()
                            + "' on '"
                            + structName
                            + "' have mismatched lengths: "
                            + e.length()
                            + " vs "
                            + i.length(),
                        typeElement
                    );
                    yield null;
                }
                yield new InlineArrayFieldModel(
                    e.name(),
                    e.elementType(),
                    e.length(),
                    e.hasGetter() || i.hasGetter(),
                    e.hasSetter() || i.hasSetter()
                );
            }
            case InlineStringFieldModel e -> {
                InlineStringFieldModel i = (InlineStringFieldModel) incoming;
                if (e.hasSetter() && i.hasSetter()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate @InlineStringField setter for field '" + e.name() + "' on '" + structName + "'",
                        typeElement
                    );
                    yield null;
                }
                if (e.length() != i.length()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineStringField getter and setter for field '"
                            + e.name()
                            + "' on '"
                            + structName
                            + "' have mismatched lengths: "
                            + e.length()
                            + " vs "
                            + i.length(),
                        typeElement
                    );
                    yield null;
                }
                yield new InlineStringFieldModel(e.name(), e.length(), e.hasGetter() || i.hasGetter(), e.hasSetter() || i.hasSetter());
            }
            default -> {
                messager.printMessage(Kind.ERROR, "Duplicate field name '" + existing.name() + "' on '" + structName + "'", typeElement);
                yield null;
            }
        };
    }

    /**
     * Turns a single abstract method on a {@code @StructSpecification} interface into a
     * {@link StructFieldModel}. Recognises {@code @ArrayField}-annotated indexed accessors,
     * {@code @InlineArrayField}-annotated fixed-size primitive array accessors,
     * {@code @InlineStringField}-annotated fixed-size C string accessors, and plain scalar
     * getters/setters. Returns {@code null} on any error.
     */
    private static StructFieldModel buildInterfaceStructField(
        ExecutableElement method,
        String enclosingStructSimpleName,
        List<String> priorStructNames,
        ProcessingEnvironment env,
        Messager messager
    ) {
        String methodName = method.getSimpleName().toString();
        AnnotationMirror arrayFieldMirror = ModelUtil.findAnnotationMirror(method, ARRAY_FIELD_FQN);
        AnnotationMirror inlineArrayMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.InlineArrayField");
        AnnotationMirror inlineStringMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.InlineStringField");

        // Enforce single annotation
        int annotationCount = (arrayFieldMirror != null ? 1 : 0) + (inlineArrayMirror != null ? 1 : 0) + (inlineStringMirror != null
            ? 1
            : 0);
        if (annotationCount > 1) {
            messager.printMessage(
                Kind.ERROR,
                "Method '"
                    + methodName
                    + "' on @StructSpecification interface '"
                    + enclosingStructSimpleName
                    + "' may not have more than one of @ArrayField, @InlineArrayField, @InlineStringField",
                method
            );
            return null;
        }

        if (arrayFieldMirror != null) {
            if (method.getParameters().size() != 1 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(Kind.ERROR, "@ArrayField method '" + methodName + "' must take a single int parameter", method);
                return null;
            }
            TypeMirror returnMirror = method.getReturnType();
            if (returnMirror.getKind() != TypeKind.DECLARED) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField method '" + methodName + "' must return a @StructSpecification record type",
                    method
                );
                return null;
            }
            TypeElement elementTypeElement = (TypeElement) env.getTypeUtils().asElement(returnMirror);
            String elementSimpleName = elementTypeElement.getSimpleName().toString();
            if (priorStructNames.contains(elementSimpleName) == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField method '"
                        + methodName
                        + "' element type '"
                        + elementSimpleName
                        + "' must be a @StructSpecification record declared in the same @LibrarySpecification interface",
                    method,
                    arrayFieldMirror
                );
                return null;
            }
            String lengthField = ModelUtil.annotationStringValue(arrayFieldMirror, "lengthField");
            if (lengthField == null || lengthField.isEmpty()) {
                messager.printMessage(Kind.ERROR, "@ArrayField on '" + methodName + "' requires lengthField", method, arrayFieldMirror);
                return null;
            }
            return new ArrayFieldModel(methodName, elementSimpleName, lengthField);
        }

        if (inlineArrayMirror != null) {
            return buildInlineArrayField(method, methodName, enclosingStructSimpleName, inlineArrayMirror, messager);
        }

        if (inlineStringMirror != null) {
            return buildInlineStringField(method, methodName, enclosingStructSimpleName, inlineStringMirror, messager);
        }

        // Check if this is a setter: void return with exactly one scalar parameter
        NativeType returnType = ModelUtil.classifyType(method.getReturnType());
        if (returnType == NativeType.VOID) {
            if (method.getParameters().size() != 1) {
                messager.printMessage(
                    Kind.ERROR,
                    "Void-return method '"
                        + methodName
                        + "' on @StructSpecification interface '"
                        + enclosingStructSimpleName
                        + "' must have exactly one parameter (setter) but has "
                        + method.getParameters().size(),
                    method
                );
                return null;
            }
            NativeType paramType = ModelUtil.classifyType(method.getParameters().get(0).asType());
            if (paramType == null
                || paramType == NativeType.VOID
                || paramType == NativeType.STRING
                || paramType == NativeType.ADDRESSABLE) {
                messager.printMessage(
                    Kind.ERROR,
                    "Setter method '"
                        + methodName
                        + "' on @StructSpecification interface '"
                        + enclosingStructSimpleName
                        + "' has unsupported parameter type '"
                        + method.getParameters().get(0).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new ScalarFieldModel(methodName, paramType, false, true);
        }

        // Scalar getter: return type is the field type, no parameters
        if (returnType == null || returnType == NativeType.STRING || returnType == NativeType.ADDRESSABLE) {
            messager.printMessage(
                Kind.ERROR,
                "Unsupported field type '"
                    + method.getReturnType()
                    + "' on method '"
                    + methodName
                    + "' of @StructSpecification interface '"
                    + enclosingStructSimpleName
                    + "'",
                method
            );
            return null;
        }
        if (method.getParameters().isEmpty() == false) {
            messager.printMessage(Kind.ERROR, "Scalar field getter '" + methodName + "' must take no parameters", method);
            return null;
        }
        return new ScalarFieldModel(methodName, returnType, true, false);
    }

    /** Returns {@code true} if {@code typeElement} directly extends {@code org.elasticsearch.foreign.Addressable}. */
    private static boolean extendsAddressable(TypeElement typeElement, ProcessingEnvironment env) {
        for (TypeMirror iface : typeElement.getInterfaces()) {
            TypeElement ifaceElement = (TypeElement) env.getTypeUtils().asElement(iface);
            if (ifaceElement != null && ifaceElement.getQualifiedName().contentEquals(ADDRESSABLE_FQN)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds an {@link InlineArrayFieldModel} (getter or setter) from a method annotated with
     * {@code @InlineArrayField}. The getter takes one {@code int} index parameter and returns a
     * primitive. The setter takes one {@code int} index and one primitive value parameter and
     * returns {@code void}. Returns {@code null} on any validation error.
     */
    private static InlineArrayFieldModel buildInlineArrayField(
        ExecutableElement method,
        String methodName,
        String enclosingStructSimpleName,
        AnnotationMirror inlineArrayMirror,
        Messager messager
    ) {
        Integer length = ModelUtil.annotationIntValue(inlineArrayMirror, "length");
        if (length == null || length <= 0) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineArrayField on '" + methodName + "' in '" + enclosingStructSimpleName + "' requires a positive length",
                method,
                inlineArrayMirror
            );
            return null;
        }

        TypeMirror returnMirror = method.getReturnType();
        boolean isVoid = returnMirror.getKind() == TypeKind.VOID;
        int paramCount = method.getParameters().size();

        if (isVoid) {
            // Setter: void fieldName(int index, T value)
            if (paramCount != 2 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must have signature void fieldName(int index, <primitive> value)",
                    method
                );
                return null;
            }
            NativeType valueType = ModelUtil.classifyType(method.getParameters().get(1).asType());
            if (valueType == null
                || valueType == NativeType.VOID
                || valueType == NativeType.STRING
                || valueType == NativeType.ADDRESSABLE
                || valueType == NativeType.ADDRESS) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' has unsupported value type '"
                        + method.getParameters().get(1).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new InlineArrayFieldModel(methodName, valueType, length, false, true);
        } else {
            // Getter: T fieldName(int index)
            if (paramCount != 1 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must have signature <primitive> fieldName(int index)",
                    method
                );
                return null;
            }
            NativeType elementType = ModelUtil.classifyType(returnMirror);
            if (elementType == null
                || elementType == NativeType.VOID
                || elementType == NativeType.STRING
                || elementType == NativeType.ADDRESSABLE
                || elementType == NativeType.ADDRESS) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must return a primitive type, got '"
                        + returnMirror
                        + "'",
                    method
                );
                return null;
            }
            return new InlineArrayFieldModel(methodName, elementType, length, true, false);
        }
    }

    /**
     * Builds an {@link InlineStringFieldModel} (getter or setter) from a method annotated with
     * {@code @InlineStringField}. The getter takes no parameters and returns {@code String}. The
     * setter takes one {@code String} parameter and returns {@code void}. Returns {@code null} on
     * any validation error.
     */
    private static InlineStringFieldModel buildInlineStringField(
        ExecutableElement method,
        String methodName,
        String enclosingStructSimpleName,
        AnnotationMirror inlineStringMirror,
        Messager messager
    ) {
        Integer length = ModelUtil.annotationIntValue(inlineStringMirror, "length");
        if (length == null || length <= 0) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineStringField on '" + methodName + "' in '" + enclosingStructSimpleName + "' requires a positive length",
                method,
                inlineStringMirror
            );
            return null;
        }

        TypeMirror returnMirror = method.getReturnType();
        boolean isVoid = returnMirror.getKind() == TypeKind.VOID;
        int paramCount = method.getParameters().size();

        if (isVoid) {
            // Setter: void fieldName(String value)
            if (paramCount != 1) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField setter '" + methodName + "' in '" + enclosingStructSimpleName + "' must have exactly one parameter",
                    method
                );
                return null;
            }
            NativeType paramType = ModelUtil.classifyType(method.getParameters().get(0).asType());
            if (paramType != NativeType.STRING) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' parameter must be String, got '"
                        + method.getParameters().get(0).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new InlineStringFieldModel(methodName, length, false, true);
        } else {
            // Getter: String fieldName()
            NativeType returnType = ModelUtil.classifyType(returnMirror);
            if (returnType != NativeType.STRING) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must return String, got '"
                        + returnMirror
                        + "'",
                    method
                );
                return null;
            }
            if (paramCount != 0) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField getter '" + methodName + "' in '" + enclosingStructSimpleName + "' must take no parameters",
                    method
                );
                return null;
            }
            return new InlineStringFieldModel(methodName, length, true, false);
        }
    }
}
