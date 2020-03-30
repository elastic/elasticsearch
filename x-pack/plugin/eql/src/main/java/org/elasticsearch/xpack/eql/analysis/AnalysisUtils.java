/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

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
        List<Attribute> matches = new ArrayList<>();

        // first take into account the qualified version
        boolean qualified = u.qualifier() != null;

        for (Attribute attribute : attrList) {
            if (!attribute.synthetic()) {
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
            return handleSpecialFields(u, matches.get(0), allowCompound);
        }

        return u.withUnresolvedMessage(
                "Reference [" + u.qualifiedName() + "] is ambiguous (to disambiguate use quotes or qualifiers); matches any of "
                        + matches.stream().map(a -> "\"" + a.qualifier() + "\".\"" + a.name() + "\"").sorted().collect(toList()));
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named, boolean allowCompound) {
        // if it's a object/compound type, keep it unresolved with a nice error message
        if (named instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) named;

            // incompatible mappings
            if (fa.field() instanceof InvalidMappedField) {
                named = u.withUnresolvedMessage("Cannot use field [" + fa.name() + "] due to ambiguities being "
                        + ((InvalidMappedField) fa.field()).errorMessage());
            }
            // unsupported types
            else if (DataTypes.isUnsupported(fa.dataType())) {
                UnsupportedEsField unsupportedField = (UnsupportedEsField) fa.field();
                if (unsupportedField.hasInherited()) {
                    named = u.withUnresolvedMessage("Cannot use field [" + fa.name() + "] with unsupported type ["
                            + unsupportedField.getOriginalType() + "] " + "in hierarchy (field [" + unsupportedField.getInherited() + "])");
                } else {
                    named = u.withUnresolvedMessage(
                            "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "]");
                }
            }
            // compound fields
            else if (allowCompound == false && DataTypes.isPrimitive(fa.dataType()) == false) {
                named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] type [" + fa.dataType().typeName() + "] only its subfields");
            }
        }
        return named;
    }
}