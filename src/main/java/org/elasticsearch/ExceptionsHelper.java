/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestStatus;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 *
 */
public final class ExceptionsHelper {

    private static final ESLogger logger = Loggers.getLogger(ExceptionsHelper.class);

    public static RuntimeException convertToRuntime(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        return new ElasticsearchException(t.getMessage(), t);
    }

    public static ElasticsearchException convertToElastic(Throwable t) {
        if (t instanceof ElasticsearchException) {
            return (ElasticsearchException) t;
        }
        return new ElasticsearchException(t.getMessage(), t);
    }

    public static RestStatus status(Throwable t) {
        if (t instanceof ElasticsearchException) {
            return ((ElasticsearchException) t).status();
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    public static Throwable unwrapCause(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof ElasticsearchWrapperException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                // dear god, if we got more than 10 levels down, WTF? just bail
                logger.warn("Exception cause unwrapping ran for 10 levels...", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    public static String detailedMessage(Throwable t) {
        return detailedMessage(t, false, 0);
    }

    public static String detailedMessage(Throwable t, boolean newLines, int initialCounter) {
        if (t == null) {
            return "Unknown";
        }
        int counter = initialCounter + 1;
        if (t.getCause() != null) {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getSimpleName());
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                if (!newLines) {
                    sb.append("; ");
                }
                t = t.getCause();
                if (t != null) {
                    if (newLines) {
                        sb.append("\n");
                        for (int i = 0; i < counter; i++) {
                            sb.append("\t");
                        }
                    } else {
                        sb.append("nested: ");
                    }
                }
                counter++;
            }
            return sb.toString();
        } else {
            return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
        }
    }

    public static String stackTrace(Throwable e) {
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(printWriter);
        return stackTraceStringWriter.toString();
    }
}
