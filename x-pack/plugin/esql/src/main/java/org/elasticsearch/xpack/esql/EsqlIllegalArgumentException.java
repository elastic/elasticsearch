/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Exception thrown when an illegal or inappropriate argument is provided to an ESQL operation.
 * <p>
 * This exception extends {@link QlIllegalArgumentException} and is used throughout the ESQL
 * query processing pipeline to indicate invalid arguments, unsupported data types, or
 * configuration errors.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing an exception for an invalid data type
 * throw EsqlIllegalArgumentException.illegalDataType(DataType.UNSUPPORTED);
 *
 * // Throwing an exception with a formatted message
 * throw new EsqlIllegalArgumentException("Invalid parameter value: {}", paramValue);
 *
 * // Wrapping another exception with context
 * try {
 *     // some operation
 * } catch (Exception e) {
 *     throw new EsqlIllegalArgumentException(e, "Failed to process: {}", input);
 * }
 * }</pre>
 */
public class EsqlIllegalArgumentException extends QlIllegalArgumentException {
    /**
     * Constructs an exception with full control over exception behavior.
     *
     * @param message the detail message
     * @param cause the cause of this exception
     * @param enableSuppression whether suppression is enabled or disabled
     * @param writableStackTrace whether the stack trace should be writable
     */
    public EsqlIllegalArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Constructs an exception with a message and a cause.
     *
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public EsqlIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with a formatted message.
     * <p>
     * The message can contain placeholders ({}) that will be replaced with the provided arguments.
     * </p>
     *
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     */
    public EsqlIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    /**
     * Constructs an exception with a cause and a formatted message.
     * <p>
     * The message can contain placeholders ({}) that will be replaced with the provided arguments.
     * </p>
     *
     * @param cause the cause of this exception
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     */
    public EsqlIllegalArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    /**
     * Constructs an exception with a simple message.
     *
     * @param message the detail message
     */
    public EsqlIllegalArgumentException(String message) {
        super(message);
    }

    /**
     * Constructs an exception with only a cause.
     *
     * @param cause the cause of this exception
     */
    public EsqlIllegalArgumentException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an exception for an illegal data type.
     *
     * @param dataType the illegal data type
     * @return an exception with a formatted message indicating the illegal data type
     */
    public static EsqlIllegalArgumentException illegalDataType(DataType dataType) {
        return EsqlIllegalArgumentException.illegalDataType(dataType.typeName());
    }

    /**
     * Creates an exception for an illegal data type name.
     *
     * @param dataTypeName the name of the illegal data type
     * @return an exception with a formatted message indicating the illegal data type
     */
    public static EsqlIllegalArgumentException illegalDataType(String dataTypeName) {
        return new EsqlIllegalArgumentException("illegal data type [" + dataTypeName + "]");
    }
}
