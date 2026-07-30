/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

/**
 * Renders one external-source scalar block value to the java form the response layer
 * ({@code PositionToXContent}) uses for the same column type, so a value reads identically wherever it surfaces:
 * IP/VERSION decode their wire bytes ({@code utf8ToString} would emit garbage), DATETIME/DATE_NANOS format as UTC
 * ISO-8601 strings rather than raw epoch longs, and UNSIGNED_LONG decodes the sign-flipped long into its numeric value.
 * <p>
 * The single source of truth for this type&rarr;value mapping. {@code _source} synthesis
 * ({@link SynthesizeExternalSource}) uses the native form directly for JSON; {@code _id} composition
 * ({@link VirtualColumnIterator}) wraps it in {@link String#valueOf} for its KEYWORD form. Types no external reader can
 * emit as a scalar fail loud, so a future type is handled intentionally rather than discovered as a corrupt value.
 */
final class ExternalScalarRenderer {

    private ExternalScalarRenderer() {}

    /** Never called with a {@code null} value — both callers render a {@code null} cell as a {@code null} column. */
    static Object render(Object value, DataType type) {
        return switch (type) {
            case KEYWORD, TEXT -> ((BytesRef) value).utf8ToString();
            case IP -> EsqlDataTypeConverter.ipToString((BytesRef) value);
            case VERSION -> EsqlDataTypeConverter.versionToString((BytesRef) value);
            case DATETIME -> EsqlDataTypeConverter.dateTimeToString((Long) value);
            case DATE_NANOS -> EsqlDataTypeConverter.nanoTimeToString((Long) value);
            case UNSIGNED_LONG -> NumericUtils.unsignedLongAsNumber((Long) value);
            case BOOLEAN, INTEGER, LONG, DOUBLE -> value;
            default -> throw new EsqlIllegalArgumentException("cannot render external scalar value of type [" + type.typeName() + "]");
        };
    }
}
