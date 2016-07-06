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

package org.elasticsearch.test;

import org.elasticsearch.test.Violation.Kind;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Method;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** class-level analyzer */
class ClassAnalyzer extends ClassVisitor {
    final String className;
    final ClassLoader loader;
    final Map<Method,MethodAnalyzer> analyses = new LinkedHashMap<>();
    boolean suppressed;
    String source;
    String superName;

    ClassAnalyzer(String className, ClassLoader loader) {
        super(CatchAnalyzer.ASM_API_VERSION);
        this.className = className;
        this.loader = loader;
    }
    
    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        this.superName = superName;
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (desc.contains("SwallowsExceptions")) {
            suppressed = true;
        }
        return null;
    }
    
    @Override
    public void visitSource(String source, String debug) {
        this.source = source;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        // don't scan bridge methods: they may have annotations, but we won't find problems!
        if ((access & Opcodes.ACC_BRIDGE) != 0) {
            return null;
        }
        MethodAnalyzer analyzer = new MethodAnalyzer(loader, className, superName, access, name, desc, signature, exceptions);
        analyses.put(new Method(name, desc), analyzer);
        return analyzer;
    }

    @Override
    public void visitEnd() {
        // fold lambdas violations into their parent methods
        List<Method> lambdas = new ArrayList<>();
        analyses.forEach((method, analysis) -> {
            for (Method lambda : analysis.lambdas) {
                MethodAnalyzer lambdaAnalysis = analyses.get(lambda);
                if (lambdaAnalysis != null) {
                    analysis.violations.addAll(lambdaAnalysis.violations);
                    lambdas.add(lambda);
                } else {
                    throw new AssertionError("where in the world is lambda " + lambda);
                }
            }
        });
        // now nuke them
        analyses.keySet().removeAll(lambdas);
    }
    
    /** log violations, grouping them by catch block entry line number for the method */
    long logViolations(PrintStream stream) {
        long violationCount = 0;
        if (suppressed) {
            long violations = 0;
            for (MethodAnalyzer analysis : analyses.values()) {
                violations += analysis.violations.size();
            }
            if (violations == 0) {
                StringBuilder sb = createMessage("Wrong class annotation", null, -1);
                addDetails(sb, new Violation(Kind.ANNOTATED_AS_SWALLOWER_BUT_HAS_NO_PROBLEMS, -1, -1));
                stream.println(sb.toString());
                violationCount++;
            }
        } else {
            for (Map.Entry<Method,MethodAnalyzer> entry : analyses.entrySet()) {
                Method method = entry.getKey();
                MethodAnalyzer analysis = entry.getValue();
                if (analysis.suppressed) {
                    if (analysis.violations.isEmpty()) {
                        StringBuilder sb = createMessage("Wrong method annotation", method, -1);
                        addDetails(sb, new Violation(Kind.ANNOTATED_AS_SWALLOWER_BUT_HAS_NO_PROBLEMS, -1, -1));
                        stream.println(sb.toString());
                        violationCount++;
                    }
                } else {
                    Map<Integer,List<Violation>> report = analysis.violations.stream()
                                                          .collect(Collectors.groupingBy(Violation::getLineNumber));
                    for (Map.Entry<Integer,List<Violation>> group : report.entrySet()) {
                        StringBuilder sb = createMessage("Broken catch block", method, group.getKey());
                        for (Violation violation : group.getValue()) {
                            addDetails(sb, violation);
                        }
                        stream.println(sb.toString());
                        violationCount++;
                    }
                }
            }
        }
        return violationCount;
    }
    
    private StringBuilder createMessage(String problem, Method method, int lineNumber) {
        StringBuilder message = new StringBuilder();
        message.append(problem);
        message.append(" at ");
        message.append(className.replace('/', '.'));
        if (method != null) {
            message.append(".");
            message.append(method.getName());
            message.append("(");
            if (source == null) {
                message.append("Unknown Source");
            } else {
                message.append(source);
            }
            if (lineNumber > 0) {
                message.append(":" + lineNumber);
            }
            message.append(")");
        }
        return message;
    }
    
    private void addDetails(StringBuilder message, Violation violation) {
        message.append(System.lineSeparator());
        message.append("\t* ");
        message.append(violation.kind.description);
        if (violation.secondaryLineNumber != -1) {
            message.append(" at line ");
            message.append(violation.secondaryLineNumber);
        }
    }
}
