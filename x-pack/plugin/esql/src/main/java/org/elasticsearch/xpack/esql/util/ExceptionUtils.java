/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.util;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.EsqlUnsupportedOperationException;
import org.elasticsearch.xpack.ql.type.DataType;

/**
 * Contains commonly used helper methods for dealing with exceptions.
 */
public class ExceptionUtils {
    /**
     * @see ExceptionUtils#illegalDataType(String)
     *
     * @param dataType the illegal data type that was encountered
     * @return an exception with a predetermined error message.
     */
    public static EsqlIllegalArgumentException illegalDataType(DataType dataType) {
        return illegalDataType(dataType.typeName());
    }

    /**
     * Create a {@link EsqlIllegalArgumentException} whose cause is an illegal data type.
     *
     * @param dataTypeName the name of the  illegal data type that was encountered
     * @return an exception with a predetermined error message.
     */
    public static EsqlIllegalArgumentException illegalDataType(String dataTypeName) {
        return new EsqlIllegalArgumentException("illegal data type [" + dataTypeName + "]");
    }

    /**
     * Create a {@link EsqlUnsupportedOperationException} whose cause is a missing method implementation.
     * @return an exception with a predetermined error message.
     */
    public static EsqlUnsupportedOperationException methodNotImplemented() {
        return new EsqlUnsupportedOperationException("method not implemented");
    }

    /**
     * @see ExceptionUtils#unsupportedDataType(String)
     *
     * @param dataType the unsupported data type that was encountered
     * @return an exception with a predetermined error message.
     */
    public static EsqlUnsupportedOperationException unsupportedDataType(DataType dataType) {
        return unsupportedDataType(dataType.typeName());
    }

    /**
     * Create a {@link EsqlUnsupportedOperationException} whose cause is an unsupported data type.
     * In contrast to an {@link EsqlIllegalArgumentException}, this is appropriate when an unsupported data type was not encountered in the
     * argument of a method, but e.g. a class' method is only implemented for a subset of data types used in the class' attributes (like an
     * expression evaluator that requires the child expressions to resolve to certain data types).
     *
     * @param dataTypeName the unsupported data type that was encountered
     * @return an exception with a predetermined error message.
     */
    public static EsqlUnsupportedOperationException unsupportedDataType(String dataTypeName) {
        return new EsqlUnsupportedOperationException("unsupported data type [" + dataTypeName + "]");
    }
}
