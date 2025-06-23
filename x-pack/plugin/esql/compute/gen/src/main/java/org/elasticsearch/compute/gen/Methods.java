/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.TypeName;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR_FIXED_BUILDER;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR_FIXED_BUILDER;
import static org.elasticsearch.compute.gen.Types.FLOAT_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.FLOAT_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.FLOAT_VECTOR_FIXED_BUILDER;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR_FIXED_BUILDER;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK_BUILDER;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR_FIXED_BUILDER;

/**
 * Finds declared methods for the code generator.
 */
public class Methods {

    static ExecutableElement requireStaticMethod(
        TypeElement declarationType,
        TypeMatcher returnTypeMatcher,
        NameMatcher nameMatcher,
        ArgumentMatcher argumentMatcher
    ) {
        ExecutableElement method = optionalStaticMethod(declarationType, returnTypeMatcher, nameMatcher, argumentMatcher);
        if (method == null) {
            var message = nameMatcher.names.size() == 1 ? "Requires method: " : "Requires one of methods: ";
            var signatures = nameMatcher.names.stream()
                .map(name -> "public static " + returnTypeMatcher + " " + declarationType + "#" + name + "(" + argumentMatcher + ")")
                .collect(joining(" or "));
            throw new IllegalArgumentException(message + signatures);
        }
        return method;
    }

    static ExecutableElement optionalStaticMethod(
        TypeElement declarationType,
        TypeMatcher returnTypeMatcher,
        NameMatcher nameMatcher,
        ArgumentMatcher argumentMatcher
    ) {
        return typeAndSuperType(declarationType).flatMap(type -> ElementFilter.methodsIn(type.getEnclosedElements()).stream())
            .filter(method -> method.getModifiers().contains(Modifier.STATIC))
            .filter(method -> nameMatcher.test(method.getSimpleName().toString()))
            .filter(method -> returnTypeMatcher.test(TypeName.get(method.getReturnType())))
            .filter(method -> argumentMatcher.test(method.getParameters().stream().map(it -> TypeName.get(it.asType())).toList()))
            .findFirst()
            .orElse(null);
    }

    static NameMatcher requireName(String... names) {
        return new NameMatcher(Set.of(names));
    }

    static TypeMatcher requireVoidType() {
        return new TypeMatcher(type -> Objects.equals(TypeName.VOID, type), "void");
    }

    static TypeMatcher requireAnyType(String description) {
        return new TypeMatcher(type -> true, description);
    }

    static TypeMatcher requirePrimitiveOrImplements(Elements elements, TypeName requiredInterface) {
        return new TypeMatcher(
            type -> type.isPrimitive() || isImplementing(elements, type, requiredInterface),
            "[boolean|int|long|float|double|" + requiredInterface + "]"
        );
    }

    static TypeMatcher requireType(TypeName requiredType) {
        return new TypeMatcher(type -> Objects.equals(requiredType, type), requiredType.toString());
    }

    static ArgumentMatcher requireAnyArgs(String description) {
        return new ArgumentMatcher(args -> true, description);
    }

    static ArgumentMatcher requireArgs(TypeMatcher... argTypes) {
        return new ArgumentMatcher(
            args -> args.size() == argTypes.length && IntStream.range(0, argTypes.length).allMatch(i -> argTypes[i].test(args.get(i))),
            Stream.of(argTypes).map(TypeMatcher::toString).collect(joining(", "))
        );
    }

    record NameMatcher(Set<String> names) implements Predicate<String> {
        @Override
        public boolean test(String name) {
            return names.contains(name);
        }
    }

    record TypeMatcher(Predicate<TypeName> matcher, String description) implements Predicate<TypeName> {
        @Override
        public boolean test(TypeName typeName) {
            return matcher.test(typeName);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    record ArgumentMatcher(Predicate<List<TypeName>> matcher, String description) implements Predicate<List<TypeName>> {
        @Override
        public boolean test(List<TypeName> typeName) {
            return matcher.test(typeName);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private static boolean isImplementing(Elements elements, TypeName type, TypeName requiredInterface) {
        return allInterfacesOf(elements, type).anyMatch(
            anInterface -> Objects.equals(anInterface.toString(), requiredInterface.toString())
        );
    }

    private static Stream<TypeName> allInterfacesOf(Elements elements, TypeName type) {
        var typeElement = elements.getTypeElement(type.toString());
        var superType = Stream.of(typeElement.getSuperclass()).filter(sType -> sType.getKind() != TypeKind.NONE).map(TypeName::get);
        var interfaces = typeElement.getInterfaces().stream().map(TypeName::get);
        return Stream.concat(
            superType.flatMap(sType -> allInterfacesOf(elements, sType)),
            interfaces.flatMap(anInterface -> Stream.concat(Stream.of(anInterface), allInterfacesOf(elements, anInterface)))
        );
    }

    private static Stream<TypeElement> typeAndSuperType(TypeElement declarationType) {
        if (declarationType.getSuperclass() instanceof DeclaredType declaredType
            && declaredType.asElement() instanceof TypeElement superType) {
            return Stream.of(declarationType, superType);
        } else {
            return Stream.of(declarationType);
        }
    }

    static ExecutableElement findMethod(TypeElement declarationType, String[] names, Predicate<ExecutableElement> filter) {
        return findMethod(names, filter, declarationType);
    }

    static ExecutableElement findMethod(String[] names, Predicate<ExecutableElement> filter, TypeElement... declarationTypes) {
        for (TypeElement declarationType : declarationTypes) {
            for (ExecutableElement e : ElementFilter.methodsIn(declarationType.getEnclosedElements())) {
                if (e.getModifiers().contains(Modifier.STATIC)) {
                    String name = e.getSimpleName().toString();
                    for (String n : names) {
                        if (n.equals(name) && filter.test(e)) {
                            return e;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns the name of the method used to add {@code valueType} instances
     * to vector or block builders.
     */
    static String appendMethod(TypeName t) {
        if (t.equals(TypeName.BOOLEAN) || t.equals(BOOLEAN_BLOCK) || t.equals(BOOLEAN_VECTOR)) {
            return "appendBoolean";
        }
        if (t.equals(Types.BYTES_REF) || t.equals(BYTES_REF_BLOCK) || t.equals(Types.BYTES_REF_VECTOR)) {
            return "appendBytesRef";
        }
        if (t.equals(TypeName.INT) || t.equals(INT_BLOCK) || t.equals(INT_VECTOR)) {
            return "appendInt";
        }
        if (t.equals(TypeName.LONG) || t.equals(LONG_BLOCK) || t.equals(LONG_VECTOR)) {
            return "appendLong";
        }
        if (t.equals(TypeName.DOUBLE) || t.equals(DOUBLE_BLOCK) || t.equals(DOUBLE_VECTOR)) {
            return "appendDouble";
        }
        if (t.equals(TypeName.FLOAT) || t.equals(FLOAT_BLOCK_BUILDER)) {
            return "appendFloat";
        }
        throw new IllegalArgumentException("unknown append method for [" + t + "]");
    }

    /**
     * Returns the name of the method used to build {@code t} instances
     * from a {@code BlockFactory}.
     */
    static String buildFromFactory(TypeName t) {
        if (t.equals(BOOLEAN_BLOCK_BUILDER)) {
            return "newBooleanBlockBuilder";
        }
        if (t.equals(BOOLEAN_VECTOR_FIXED_BUILDER)) {
            return "newBooleanVectorFixedBuilder";
        }
        if (t.equals(BOOLEAN_VECTOR_BUILDER)) {
            return "newBooleanVectorBuilder";
        }
        if (t.equals(BYTES_REF_BLOCK_BUILDER)) {
            return "newBytesRefBlockBuilder";
        }
        if (t.equals(BYTES_REF_VECTOR_BUILDER)) {
            return "newBytesRefVectorBuilder";
        }
        if (t.equals(INT_BLOCK_BUILDER)) {
            return "newIntBlockBuilder";
        }
        if (t.equals(INT_VECTOR_FIXED_BUILDER)) {
            return "newIntVectorFixedBuilder";
        }
        if (t.equals(INT_VECTOR_BUILDER)) {
            return "newIntVectorBuilder";
        }
        if (t.equals(LONG_BLOCK_BUILDER)) {
            return "newLongBlockBuilder";
        }
        if (t.equals(LONG_VECTOR_FIXED_BUILDER)) {
            return "newLongVectorFixedBuilder";
        }
        if (t.equals(LONG_VECTOR_BUILDER)) {
            return "newLongVectorBuilder";
        }
        if (t.equals(DOUBLE_BLOCK_BUILDER)) {
            return "newDoubleBlockBuilder";
        }
        if (t.equals(DOUBLE_VECTOR_BUILDER)) {
            return "newDoubleVectorBuilder";
        }
        if (t.equals(DOUBLE_VECTOR_FIXED_BUILDER)) {
            return "newDoubleVectorFixedBuilder";
        }
        if (t.equals(FLOAT_BLOCK_BUILDER)) {
            return "newFloatBlockBuilder";
        }
        if (t.equals(FLOAT_VECTOR_BUILDER)) {
            return "newFloatVectorBuilder";
        }
        if (t.equals(FLOAT_VECTOR_FIXED_BUILDER)) {
            return "newFloatVectorFixedBuilder";
        }
        throw new IllegalArgumentException("unknown build method for [" + t + "]");
    }

    /**
     * Returns the name of the method used to get {@code valueType} instances
     * from vectors or blocks.
     */
    static String getMethod(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return "getBoolean";
        }
        if (elementType.equals(Types.BYTES_REF)) {
            return "getBytesRef";
        }
        if (elementType.equals(TypeName.INT)) {
            return "getInt";
        }
        if (elementType.equals(TypeName.LONG)) {
            return "getLong";
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return "getDouble";
        }
        if (elementType.equals(TypeName.FLOAT)) {
            return "getFloat";
        }
        throw new IllegalArgumentException("unknown get method for [" + elementType + "]");
    }

    /**
     * Returns the name of the method used to get {@code valueType} instances
     * from vectors or blocks.
     */
    static String vectorAccessorName(String elementTypeName) {
        return switch (elementTypeName) {
            case "BOOLEAN" -> "getBoolean";
            case "INT" -> "getInt";
            case "LONG" -> "getLong";
            case "DOUBLE" -> "getDouble";
            case "FLOAT" -> "getFloat";
            case "BYTES_REF" -> "getBytesRef";
            default -> throw new IllegalArgumentException(
                "don't know how to fetch primitive values from " + elementTypeName + ". define combineIntermediate."
            );
        };
    }
}
