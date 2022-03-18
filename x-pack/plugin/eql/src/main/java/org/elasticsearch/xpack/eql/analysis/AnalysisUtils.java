/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.expression.OptionalResolvedAttribute;
import org.elasticsearch.xpack.eql.expression.OptionalUnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public final class AnalysisUtils {

    private AnalysisUtils() {}

    //
    // Shared methods around the analyzer rules
    //
    static Attribute resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList) {
        return resolveAgainstList(u, attrList, false);
    }

    static Attribute resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList, boolean allowCompound) {
        Set<Attribute> matches = new LinkedHashSet<>();

        // first take into account the qualified version
        boolean qualified = u.qualifier() != null;

        for (Attribute attribute : attrList) {
            if (attribute.synthetic() == false) {
                boolean match = qualified ? Objects.equals(u.qualifiedName(), attribute.qualifiedName()) :
                // if the field is unqualified
                // first check the names directly
                    (Objects.equals(u.name(), attribute.name())
                        // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                        || Objects.equals(u.name(), attribute.qualifiedName()));
                if (match) {
                    matches.add(attribute.withLocation(u.source()));
                }
            }
        }

        // none found
        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1) {
            return handleSpecialFields(u, matches.iterator().next(), allowCompound);
        }

        return u.withUnresolvedMessage(
            "Reference ["
                + u.qualifiedName()
                + "] is ambiguous (to disambiguate use quotes or qualifiers); matches any of "
                + matches.stream().map(a -> "\"" + a.qualifier() + "\".\"" + a.name() + "\"").sorted().collect(toList())
        );
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named, boolean allowCompound) {
        // if it's a object/compound type, keep it unresolved with a nice error message
        if (named instanceof FieldAttribute fa) {

            // incompatible mappings
            if (fa.field()instanceof InvalidMappedField field) {
                named = u.withUnresolvedMessage("Cannot use field [" + fa.name() + "] due to ambiguities being " + field.errorMessage());
            }
            // unsupported types
            else if (DataTypes.isUnsupported(fa.dataType())) {
                UnsupportedEsField unsupportedField = (UnsupportedEsField) fa.field();
                if (unsupportedField.hasInherited()) {
                    named = u.withUnresolvedMessage(
                        "Cannot use field ["
                            + fa.name()
                            + "] with unsupported type ["
                            + unsupportedField.getOriginalType()
                            + "] "
                            + "in hierarchy (field ["
                            + unsupportedField.getInherited()
                            + "])"
                    );
                } else {
                    named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "]"
                    );
                }
            }
            // compound fields that are not of "nested" type
            else if (allowCompound == false && DataTypes.isPrimitive(fa.dataType()) == false && fa.dataType() != DataTypes.NESTED) {
                named = u.withUnresolvedMessage(
                    "Cannot use field [" + fa.name() + "] type [" + fa.dataType().typeName() + "] only its subfields"
                );
            }
            // "nested" fields
            else if (fa.dataType() == DataTypes.NESTED) {
                named = u.withUnresolvedMessage(
                    "Cannot use field ["
                        + fa.name()
                        + "] type ["
                        + fa.dataType().typeName()
                        + "] "
                        + "due to nested fields not being supported yet"
                );
            }
            // fields having nested parents
            else if (fa.isNested()) {
                named = u.withUnresolvedMessage(
                    "Cannot use field ["
                        + fa.name()
                        + "] type ["
                        + fa.dataType().typeName()
                        + "] "
                        + "with unsupported nested type in hierarchy (field ["
                        + fa.nestedParent().name()
                        + "])"
                );
            }
            // remember if the field was optional - needed when generating the runtime query
            else if (u instanceof OptionalUnresolvedAttribute) {
                named = new OptionalResolvedAttribute(fa);
            }
        } else {
            if (u instanceof OptionalUnresolvedAttribute) {
                named = u.withUnresolvedMessage("Unsupported optional field [" + named.name() + "] type [" + named.dataType().typeName());
            }
        }
        return named;
    }
}
