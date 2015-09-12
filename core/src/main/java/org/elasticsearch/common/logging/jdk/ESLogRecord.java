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

package org.elasticsearch.common.logging.jdk;

import org.elasticsearch.common.logging.support.AbstractESLogger;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * A {@link LogRecord} which is used in conjunction with {@link JdkESLogger}
 * with the ability to provide the class name, method name and line number
 * information of the code calling the logger
 */
public class ESLogRecord extends LogRecord {

    private static final long serialVersionUID = 1107741560233585726L;
    private static final String FQCN = AbstractESLogger.class.getName();
    private String sourceClassName;
    private String sourceMethodName;
    private transient boolean needToInferCaller;

    public ESLogRecord(Level level, String msg) {
        super(level, msg);
        needToInferCaller = true;
    }

    @Override
    public String getSourceClassName() {
        if (needToInferCaller) {
            inferCaller();
        }
        return sourceClassName;
    }

    @Override
    public void setSourceClassName(String sourceClassName) {
        this.sourceClassName = sourceClassName;
        needToInferCaller = false;
    }

    @Override
    public String getSourceMethodName() {
        if (needToInferCaller) {
            inferCaller();
        }
        return sourceMethodName;
    }

    @Override
    public void setSourceMethodName(String sourceMethodName) {
        this.sourceMethodName = sourceMethodName;
        needToInferCaller = false;
    }

    /**
     * Determines the source information for the caller of the logger (class
     * name, method name, and line number)
     */
    private void inferCaller() {
        needToInferCaller = false;
        Throwable throwable = new Throwable();

        boolean lookingForLogger = true;
        for (final StackTraceElement frame : throwable.getStackTrace()) {
            String cname = frame.getClassName();
            boolean isLoggerImpl = isLoggerImplFrame(cname);
            if (lookingForLogger) {
                // Skip all frames until we have found the first logger frame.
                if (isLoggerImpl) {
                    lookingForLogger = false;
                }
            } else {
                if (!isLoggerImpl) {
                    // skip reflection call
                    if (!cname.startsWith("java.lang.reflect.") && !cname.startsWith("sun.reflect.")) {
                       // We've found the relevant frame.
                       setSourceClassName(cname);
                       setSourceMethodName(frame.getMethodName());
                       return;
                    }
                }
            }
        }
        // We haven't found a suitable frame, so just punt.  This is
        // OK as we are only committed to making a "best effort" here.
    }

    private boolean isLoggerImplFrame(String cname) {
        // the log record could be created for a platform logger
        return cname.equals(FQCN);
    }
}
