/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.DefaultSymbolResolver;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.SymbolResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Models a {@code @LibrarySpecification}-annotated interface and the methods that will be bound to
 * native symbols. The supported surface is intentionally narrow: every abstract method must be
 * annotated with {@code @Function} or {@code @StructFactory}; parameter types are limited to
 * primitives and {@code MemorySegment}; return types may also be {@code String}.
 *
 * @param qualifiedName the fully-qualified interface name
 * @param simpleName the simple interface name
 * @param packageName the package name (may be empty)
 * @param libraryName the native library name from {@code @LibrarySpecification.name()} (may be empty)
 * @param methods all native methods in declaration order
 * @param unavailableOn enum constant names of platforms where this library is unavailable (empty means available everywhere)
 * @param structs all {@code @StructSpecification} types enclosed in this interface, in declaration order
 * @param symbolResolverClassName fully-qualified name of the {@link SymbolResolver} implementation
 *        (defaults to {@code org.elasticsearch.foreign.DefaultSymbolResolver})
 */
public record LibraryModel(
    String qualifiedName,
    String simpleName,
    String packageName,
    String libraryName,
    List<MethodModel> methods,
    List<String> unavailableOn,
    List<StructModel> structs,
    String symbolResolverClassName
) {

    /** All known platform names — used to detect a library that can never be natively loaded. */
    private static final Set<String> ALL_PLATFORM_NAMES = Set.of(
        "LINUX_X64",
        "LINUX_AARCH64",
        "DARWIN_X64",
        "DARWIN_AARCH64",
        "WINDOWS_X64"
    );

    public static final String RESOLVER_INTERFACE_FQN = SymbolResolver.class.getName();
    public static final String DEFAULT_RESOLVER_FQN = DefaultSymbolResolver.class.getName();
    public static final String LIBRARY_SPECIFICATION_FQN = LibrarySpecification.class.getName();

    /** Fully-qualified name of the {@code $Impl} class generated for this library. */
    public String implQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Impl" : packageName + "." + simpleName + "$Impl";
    }

    /** Fully-qualified name of the {@code $Provider} class generated for this library. */
    public String providerQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Provider" : packageName + "." + simpleName + "$Provider";
    }

    /**
     * Builds a {@code LibraryModel} from a {@code @LibrarySpecification}-annotated interface element.
     * Emits {@link Kind#ERROR} diagnostics via the messager for any validation failure.
     *
     * @return the built model, or null if any error was emitted
     */
    public static LibraryModel from(TypeElement element, ProcessingEnvironment env) {
        Messager messager = env.getMessager();

        if (element.getKind() != ElementKind.INTERFACE) {
            messager.printMessage(Kind.ERROR, "@LibrarySpecification must be on an interface", element);
            return null;
        }

        LibrarySpecification annotation = element.getAnnotation(LibrarySpecification.class);
        String libraryName = annotation != null ? annotation.name() : "";
        String qualifiedName = element.getQualifiedName().toString();
        String simpleName = element.getSimpleName().toString();
        String packageName = env.getElementUtils().getPackageOf(element).getQualifiedName().toString();

        AnnotationMirror specMirror = ModelUtil.findAnnotationMirror(element, LIBRARY_SPECIFICATION_FQN);
        List<String> unavailableOn = extractUnavailableOn(specMirror);

        boolean hasError = false;
        if (unavailableOn.containsAll(ALL_PLATFORM_NAMES)) {
            messager.printMessage(
                Kind.ERROR,
                "@LibrarySpecification.unavailableOn lists all known platforms; the library will never be natively loaded",
                element,
                specMirror
            );
            hasError = true;
        }

        String symbolResolverClassName = resolveAndValidateSymbolResolver(element, messager, env.getTypeUtils());
        if (symbolResolverClassName == null) {
            hasError = true;
        }

        // First pass: collect struct specifications in declaration order
        List<StructModel> structs = new ArrayList<>();
        List<String> structSimpleNames = new ArrayList<>();
        for (var enclosed : element.getEnclosedElements()) {
            ElementKind kind = enclosed.getKind();
            boolean isType = kind == ElementKind.RECORD
                || kind == ElementKind.INTERFACE
                || kind == ElementKind.CLASS
                || kind == ElementKind.ENUM
                || kind == ElementKind.ANNOTATION_TYPE;
            if (isType == false) {
                continue;
            }
            TypeElement typeElement = (TypeElement) enclosed;
            AnnotationMirror structSpecMirror = ModelUtil.findAnnotationMirror(
                typeElement,
                "org.elasticsearch.foreign.StructSpecification"
            );
            if (structSpecMirror == null) {
                continue;
            }
            if (kind != ElementKind.RECORD && kind != ElementKind.INTERFACE) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructSpecification is only allowed on a record or interface",
                    enclosed,
                    structSpecMirror
                );
                hasError = true;
                continue;
            }

            StructModel structModel = kind == ElementKind.RECORD
                ? buildRecordStructModel(typeElement, messager)
                : buildInterfaceStructModel(typeElement, structSimpleNames, env, messager);
            if (structModel == null) {
                hasError = true;
            } else {
                structs.add(structModel);
                structSimpleNames.add(structModel.simpleName());
            }
        }

        // Second pass: collect methods (skipping struct declarations)
        List<MethodModel> methods = new ArrayList<>();
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosed;
            if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }

            MethodModel methodModel = MethodModel.from(method, env, structSimpleNames);
            if (methodModel == null) {
                hasError = true;
            } else {
                methods.add(methodModel);
            }
        }

        return hasError
            ? null
            : new LibraryModel(
                qualifiedName,
                simpleName,
                packageName,
                libraryName,
                methods,
                unavailableOn,
                structs,
                symbolResolverClassName
            );
    }

    /**
     * Resolves and validates the {@code symbolResolver} attribute from {@link LibrarySpecification}.
     * Returns the default ({@link DefaultSymbolResolver}) when no custom resolver is specified.
     * The resolver class must implement {@link SymbolResolver} and have a public no-arg constructor.
     *
     * @return the resolver's fully-qualified name (never null on success), or {@code null} if validation failed
     *         (error already emitted).
     */
    private static String resolveAndValidateSymbolResolver(TypeElement element, Messager messager, Types types) {
        AnnotationMirror specMirror = ModelUtil.findAnnotationMirror(element, LIBRARY_SPECIFICATION_FQN);
        if (specMirror == null) {
            return DEFAULT_RESOLVER_FQN;
        }

        TypeMirror resolverTypeMirror = ModelUtil.annotationClassValue(specMirror, "symbolResolver");
        if (resolverTypeMirror == null) {
            return DEFAULT_RESOLVER_FQN;
        }

        TypeElement resolverElement = types.asElement(resolverTypeMirror) instanceof TypeElement te ? te : null;
        if (resolverElement == null) {
            messager.printMessage(Kind.ERROR, "symbolResolver must reference a class", element, specMirror);
            return null;
        }

        // Use the JVM binary name (e.g. "pkg.Enclosing$Nested" for nested classes), not the
        // dot-separated qualified name, since the generator emits this into bytecode.
        String resolverFqn = binaryName(resolverElement);

        if (resolverFqn.equals(DEFAULT_RESOLVER_FQN)) {
            return DEFAULT_RESOLVER_FQN;
        }

        TypeElement resolverInterface = findTypeElement(resolverElement, RESOLVER_INTERFACE_FQN);
        if (resolverInterface == null) {
            messager.printMessage(
                Kind.ERROR,
                "symbolResolver class [" + resolverFqn + "] must implement [" + RESOLVER_INTERFACE_FQN + "]",
                element,
                specMirror
            );
            return null;
        }

        if (hasPublicNoArgConstructor(resolverElement) == false) {
            messager.printMessage(
                Kind.ERROR,
                "symbolResolver class [" + resolverFqn + "] must have a public no-arg constructor",
                element,
                specMirror
            );
            return null;
        }

        return resolverFqn;
    }

    /** Checks whether the given type implements (directly or transitively) the interface with the given FQN. */
    private static TypeElement findTypeElement(TypeElement type, String interfaceFqn) {
        for (TypeMirror iface : type.getInterfaces()) {
            if (iface.getKind() != TypeKind.DECLARED) {
                continue;
            }
            TypeElement ifaceElement = (TypeElement) ((DeclaredType) iface).asElement();
            if (ifaceElement.getQualifiedName().contentEquals(interfaceFqn)) {
                return ifaceElement;
            }
            TypeElement found = findTypeElement(ifaceElement, interfaceFqn);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Returns the JVM binary name for a type element — e.g. {@code "pkg.Outer$Inner"} for a nested
     * class. {@link TypeElement#getQualifiedName()} uses a dot between the enclosing type and the
     * nested type, which is wrong when the name gets baked into bytecode.
     */
    private static String binaryName(TypeElement type) {
        StringBuilder name = new StringBuilder(type.getSimpleName());
        var enclosing = type.getEnclosingElement();
        while (enclosing instanceof TypeElement enclosingType) {
            name.insert(0, enclosingType.getSimpleName() + "$");
            enclosing = enclosingType.getEnclosingElement();
        }
        if (enclosing instanceof javax.lang.model.element.PackageElement pkg && pkg.isUnnamed() == false) {
            name.insert(0, pkg.getQualifiedName() + ".");
        }
        return name.toString();
    }

    private static boolean hasPublicNoArgConstructor(TypeElement type) {
        for (var enclosed : type.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.CONSTRUCTOR) {
                continue;
            }
            ExecutableElement ctor = (ExecutableElement) enclosed;
            if (ctor.getParameters().isEmpty() && ctor.getModifiers().contains(Modifier.PUBLIC)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts the {@code unavailableOn} attribute from the {@code @LibrarySpecification} annotation mirror
     * as a list of enum constant names. Uses annotation mirror APIs to avoid loading the {@code Platform}
     * class at processing time. Pure extraction — validation is the caller's responsibility.
     */
    private static List<String> extractUnavailableOn(AnnotationMirror specMirror) {
        if (specMirror == null) {
            return List.of();
        }

        for (var entry : specMirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals("unavailableOn") == false) {
                continue;
            }
            Object rawValue = entry.getValue().getValue();
            if ((rawValue instanceof List<?>) == false) {
                return List.of();
            }
            List<?> valueList = (List<?>) rawValue;
            List<String> platformNames = new ArrayList<>();
            for (Object item : valueList) {
                if (item instanceof AnnotationValue av && av.getValue() instanceof VariableElement ve) {
                    platformNames.add(ve.getSimpleName().toString());
                }
            }
            return List.copyOf(platformNames);
        }
        return List.of();
    }

    /**
     * Builds a {@link StructModel} for a {@code @StructSpecification} record. Emits errors for any
     * unsupported record component types and returns {@code null} if any error was emitted.
     */
    private static StructModel buildRecordStructModel(TypeElement typeElement, Messager messager) {
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
    private static StructModel buildInterfaceStructModel(
        TypeElement typeElement,
        List<String> priorStructNames,
        ProcessingEnvironment env,
        Messager messager
    ) {
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
        AnnotationMirror arrayFieldMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.ArrayField");
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
