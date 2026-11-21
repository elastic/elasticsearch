/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * The Parquet {@code LIST} that a multi-valued field becomes, in the standard three-level layout that every reader understands:
 * <pre>
 * optional group f (LIST) {
 *   repeated group list {
 *     optional &lt;type&gt; element;
 *   }
 * }
 * </pre>
 * The list is null for a document that has no value for the field (a missing field, {@code null} and {@code []} are the same for the
 * batch path), and an element is null for a null element of an array of keywords. The element has the physical and the logical type that
 * the field has when it is single-valued.
 */
public final class ParquetLists {

    public static final String LIST = "list";
    public static final String ELEMENT = "element";

    private ParquetLists() {}

    public static boolean isList(Type type) {
        return type.isPrimitive() == false && type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }

    /** The element of a list column. */
    public static PrimitiveType elementOf(Type list) {
        return list.asGroupType().getType(LIST).asGroupType().getType(ELEMENT).asPrimitiveType();
    }

    /** The list column with the name, of elements of the type of {@code element} (whose own name and id are not used). */
    public static GroupType listOf(String name, PrimitiveType.PrimitiveTypeName physical, LogicalTypeAnnotation logical) {
        return listOf(name, physical, logical, null);
    }

    /** The list column whose element has the field id {@code elementId}, or no id when it is null. */
    public static GroupType listOf(
        String name,
        PrimitiveType.PrimitiveTypeName physical,
        LogicalTypeAnnotation logical,
        Integer elementId
    ) {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(physical);
        if (logical != null) {
            builder = builder.as(logical);
        }
        if (elementId != null) {
            builder = builder.id(elementId);
        }
        final PrimitiveType element = builder.named(ELEMENT);
        return Types.optionalGroup()
            .as(LogicalTypeAnnotation.listType())
            .addField(Types.repeatedGroup().addField(element).named(LIST))
            .named(name);
    }
}
