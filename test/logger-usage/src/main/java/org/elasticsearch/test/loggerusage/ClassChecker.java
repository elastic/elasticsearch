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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.IGNORE_CHECKS_ANNOTATION;

public class ClassChecker extends ClassVisitor {

    private final Consumer<WrongLoggerUsage> wrongUsageCallback;
    private final Predicate<String> methodsToCheck;
    private String className;
    private boolean ignoreChecks;

    public ClassChecker(Consumer<WrongLoggerUsage> wrongUsageCallback, Predicate<String> methodsToCheck) {
        super(Opcodes.ASM7);
        this.wrongUsageCallback = wrongUsageCallback;
        this.methodsToCheck = methodsToCheck;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        this.className = name;
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (IGNORE_CHECKS_ANNOTATION.equals(Type.getType(desc).getClassName())) {
            ignoreChecks = true;
        }
        return super.visitAnnotation(desc, visible);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        if (!ignoreChecks && methodsToCheck.test(name)) {
            return new MethodChecker(this.className, access, name, desc, wrongUsageCallback);
        } else {
            return super.visitMethod(access, name, desc, signature, exceptions);
        }
    }
}
