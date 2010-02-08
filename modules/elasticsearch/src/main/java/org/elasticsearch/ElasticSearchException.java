/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;

/**
 * @author kimchy (Shay Banon) (Shay Banon)
 */
public class ElasticSearchException extends RuntimeException {

    /**
     * Construct a <code>NestedRuntimeException</code> with the specified detail message.
     *
     * @param msg the detail message
     */
    public ElasticSearchException(String msg) {
        super(msg);
    }

    /**
     * Construct a <code>NestedRuntimeException</code> with the specified detail message
     * and nested exception.
     *
     * @param msg   the detail message
     * @param cause the nested exception
     */
    public ElasticSearchException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            if (super.getMessage() != null) {
                sb.append(super.getMessage()).append("; ");
            }
            sb.append("nested exception is ").append(getCause());
            return sb.toString();
        } else {
            return super.getMessage();
        }
    }


    /**
     * Retrieve the innermost cause of this exception, if any.
     *
     * @return the innermost exception, or <code>null</code> if none
     * @since 2.0
     */
    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    /**
     * Retrieve the most specific cause of this exception, that is,
     * either the innermost cause (root cause) or this exception itself.
     * <p>Differs from {@link #getRootCause()} in that it falls back
     * to the present exception if there is no root cause.
     *
     * @return the most specific cause (never <code>null</code>)
     * @since 2.0.3
     */
    public Throwable getMostSpecificCause() {
        Throwable rootCause = getRootCause();
        return (rootCause != null ? rootCause : this);
    }

    /**
     * Check whether this exception contains an exception of the given type:
     * either it is of the given class itself or it contains a nested cause
     * of the given type.
     *
     * @param exType the exception type to look for
     * @return whether there is a nested exception of the specified type
     */
    public boolean contains(Class exType) {
        if (exType == null) {
            return false;
        }
        if (exType.isInstance(this)) {
            return true;
        }
        Throwable cause = getCause();
        if (cause == this) {
            return false;
        }
        if (cause instanceof ElasticSearchException) {
            return ((ElasticSearchException) cause).contains(exType);
        } else {
            while (cause != null) {
                if (exType.isInstance(cause)) {
                    return true;
                }
                if (cause.getCause() == cause) {
                    break;
                }
                cause = cause.getCause();
            }
            return false;
        }
    }
}
