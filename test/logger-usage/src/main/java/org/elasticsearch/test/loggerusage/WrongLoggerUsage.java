/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.loggerusage;

import org.objectweb.asm.Type;

import static org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.LOGGER_CLASS;

public class WrongLoggerUsage {

    private final String className;
    private final String methodName;
    private final String logMethodName;
    private final int line;
    private final String errorMessage;

    public WrongLoggerUsage(String className, String methodName, String logMethodName, int line, String errorMessage) {
        this.className = className;
        this.methodName = methodName;
        this.logMethodName = logMethodName;
        this.line = line;
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "WrongLoggerUsage{" +
            "className='" + className + '\'' +
            ", methodName='" + methodName + '\'' +
            ", logMethodName='" + logMethodName + '\'' +
            ", line=" + line +
            ", errorMessage='" + errorMessage + '\'' +
            '}';
    }

    /**
     * Returns an error message that has the form of stack traces emitted by {@link Throwable#printStackTrace()}
     */
    public String getErrorLines() {
        String fullClassName = Type.getObjectType(className).getClassName();
        String simpleClassName = fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
        int innerClassIndex = simpleClassName.indexOf('$');
        if (innerClassIndex > 0) {
            simpleClassName = simpleClassName.substring(0, innerClassIndex);
        }
        simpleClassName = simpleClassName + ".java";
        return "Bad usage of " + LOGGER_CLASS + "#" + logMethodName + ": " + errorMessage +
            "\n\tat " + fullClassName + "." + methodName + "(" + simpleClassName + ":" + line + ")";
    }
}
