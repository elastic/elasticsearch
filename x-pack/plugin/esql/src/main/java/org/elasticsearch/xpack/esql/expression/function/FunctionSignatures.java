/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and expands {@link FunctionInfo#signatures()} into concrete type tuples.
 * <p>
 *     Parameter entries may use {@code |} for unions and {@link TypeGroup} names
 *     (e.g. {@code NUMERIC}, {@code STRING}, {@code GEO}, {@code SORTABLE}, {@code ALL}).
 *     {@link Signature#returnType()} must be either a single concrete type name or a
 *     positional reference {@code $N} (return type follows parameter {@code N} after
 *     expansion, normalized with {@link DataType#noText()}). Type groups and {@code |}
 *     unions are not allowed in the return type.
 * </p>
 */
public final class FunctionSignatures {
    private static final Pattern RETURN_PARAM_REF = Pattern.compile("\\$(\\d+)");

    private FunctionSignatures() {}

    /**
     * A fully expanded signature: one concrete type per argument, plus return type.
     */
    public record ConcreteSignature(List<DataType> argTypes, DataType returnType) {}

    /**
     * Declared signatures for {@code def}, or an empty set when the function has not
     * opted into {@link FunctionInfo#signatures()}.
     */
    public static Set<ConcreteSignature> declaredSignatures(FunctionDefinition def) {
        FunctionInfo info = EsqlFunctionRegistry.functionInfo(def);
        if (info == null || info.signatures().length == 0) {
            return Set.of();
        }
        return expand(info.signatures());
    }

    /**
     * Expands each {@link Signature} into concrete signatures.
     */
    public static Set<ConcreteSignature> expand(Signature[] signatures) {
        Set<ConcreteSignature> result = new LinkedHashSet<>();
        for (Signature signature : signatures) {
            result.addAll(expand(signature));
        }
        return result;
    }

    /**
     * Expands a single {@link Signature} into concrete signatures.
     */
    public static Set<ConcreteSignature> expand(Signature signature) {
        return new LinkedHashSet<>(expandOne(signature));
    }

    private static List<ConcreteSignature> expandOne(Signature signature) {
        String returnDecl = signature.returnType().trim();
        List<List<DataType>> perPosition = new ArrayList<>(signature.params().length);
        for (String param : signature.params()) {
            perPosition.add(resolveParam(param));
        }

        Matcher ref = RETURN_PARAM_REF.matcher(returnDecl);
        if (ref.matches()) {
            int index = Integer.parseInt(ref.group(1));
            if (index < 0 || index >= signature.params().length) {
                throw new IllegalArgumentException(
                    "return type reference [" + returnDecl + "] is out of range for " + signature.params().length + " parameter(s)"
                );
            }
            return expandWithReturnRef(perPosition, index);
        }

        if (returnDecl.contains("|")) {
            throw new IllegalArgumentException("return type [" + returnDecl + "] must be a concrete type or $N reference, not a union");
        }
        if (TypeGroup.parse(returnDecl) != null) {
            throw new IllegalArgumentException(
                "return type [" + returnDecl + "] must be a concrete type or $N reference, not a type group"
            );
        }

        DataType returnType = requireKnownType(returnDecl, "return type");
        List<ConcreteSignature> expanded = new ArrayList<>();
        expandRecursive(perPosition, 0, new ArrayList<>(perPosition.size()), returnType, null, expanded);
        return expanded;
    }

    /**
     * Expand params freely; for each concrete arg list, return type is
     * {@code args.get(index).noText()}.
     */
    private static List<ConcreteSignature> expandWithReturnRef(List<List<DataType>> perPosition, int index) {
        List<ConcreteSignature> expanded = new ArrayList<>();
        expandRecursive(perPosition, 0, new ArrayList<>(perPosition.size()), null, index, expanded);
        return expanded;
    }

    private static List<DataType> resolveParam(String param) {
        List<DataType> types = new ArrayList<>();
        for (String part : param.split("\\|")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("empty type in signature param [" + param + "]");
            }
            types.addAll(resolveToken(trimmed));
        }
        return types;
    }

    private static List<DataType> resolveToken(String token) {
        TypeGroup group = TypeGroup.parse(token);
        if (group != null) {
            return group.types();
        }
        return List.of(requireKnownType(token, "parameter type"));
    }

    private static DataType requireKnownType(String typeName, String kind) {
        DataType type = DataType.fromNameOrAlias(typeName.toLowerCase(Locale.ROOT));
        if (type == DataType.UNSUPPORTED) {
            throw new IllegalArgumentException(kind + " [" + typeName + "] is not a known data type");
        }
        return type;
    }

    /**
     * @param fixedReturn non-null when return type is concrete; then {@code returnRefIndex} is ignored
     * @param returnRefIndex non-null when return type is {@code $N}; then {@code fixedReturn} is ignored
     */
    private static void expandRecursive(
        List<List<DataType>> perPosition,
        int index,
        List<DataType> current,
        DataType fixedReturn,
        Integer returnRefIndex,
        List<ConcreteSignature> out
    ) {
        if (index == perPosition.size()) {
            DataType returnType = fixedReturn != null ? fixedReturn : current.get(returnRefIndex).noText();
            out.add(new ConcreteSignature(List.copyOf(current), returnType));
            return;
        }
        for (DataType type : perPosition.get(index)) {
            current.add(type);
            expandRecursive(perPosition, index + 1, current, fixedReturn, returnRefIndex, out);
            current.remove(current.size() - 1);
        }
    }
}
