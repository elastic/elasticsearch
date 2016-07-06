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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TryCatchBlockNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

import java.lang.invoke.LambdaMetafactory;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Scans a single method for violations */
class MethodAnalyzer extends MethodVisitor {
    /** classloader (for type resolution) */
    final ClassLoader loader;
    /** method name */
    final String name;
    /** parent class name */
    final String owner;
    /** parent class's superclass */
    final String superName;
    /** any violations we found for this method */
    final Set<Violation> violations = new TreeSet<>();
    /** any lambda invocations we found for this method (we'll apply suppression to them, if needed) */
    final List<Method> lambdas = new ArrayList<>();
    /** true if we found a suppression annotation for this method */
    boolean suppressed;
    
    MethodAnalyzer(ClassLoader loader, String owner, String superName, 
                   int access, String name, String desc, String signature, String[] exceptions) {
        super(CatchAnalyzer.ASM_API_VERSION, new MethodNode(access, name, desc, signature, exceptions));
        this.loader = loader;
        this.name = name;
        this.owner = owner;
        this.superName = superName;
    }
    
    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (desc.contains("SwallowsExceptions")) {
            suppressed = true;
        }
        return null;
    }
    
    private static final String LAMBDA_META_FACTORY_INTERNAL_NAME = Type.getInternalName(LambdaMetafactory.class);

    @Override
    public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
        // if we see calls to LambdaMetaFactory, remember them, and later we will fold analysis
        // of the desugared method back into this method
        if (LAMBDA_META_FACTORY_INTERNAL_NAME.equals(bsm.getOwner())) {
            Handle implMethod = (Handle) bsmArgs[1];
            if (implMethod.getOwner().equals(owner) && implMethod.getName().startsWith("lambda$")) {
                lambdas.add(new Method(implMethod.getName(), implMethod.getDesc()));
            }
        }
        super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
    }

    @Override
    public void visitEnd() {
        MethodNode node = (MethodNode)mv;
        AbstractInsnNode insns[] = node.instructions.toArray(); // all instructions for the method
        Set<Integer> handlers = new TreeSet<>(); // entry points of exception handlers found

        Analyzer<BasicValue> a = new Analyzer<BasicValue>(new ThrowableInterpreter(loader)) {
            @Override
            protected Frame<BasicValue> newFrame(Frame<? extends BasicValue> src) {
                return new Node(src, MethodAnalyzer.this);
            }
            
            @Override
            protected Frame<BasicValue> newFrame(int nLocals, int nStack) {
                return new Node(nLocals, nStack, MethodAnalyzer.this);
            }
            
            @Override
            protected void newControlFlowEdge(int insn, int next) {
                Node s = (Node) getFrames()[insn];
                s.edges.add(next);
            }
            
            @Override
            protected boolean newControlFlowExceptionEdge(int insn, TryCatchBlockNode next) {
                int nextInsn = node.instructions.indexOf(next.handler);
                newControlFlowEdge(insn, nextInsn);
                // null type: finally block
                if (next.type != null) {
                    handlers.add(nextInsn);
                }
                return true;
            }
        };
        try {
            Frame<BasicValue> frames[] = a.analyze(owner, node);
            List<Node> nodes = new ArrayList<>();
            for (Frame<BasicValue> frame : frames) {
                nodes.add((Node)frame);
            }
            // check the destination of every exception edge
            for (int handler : handlers) {
                int line = getLineNumberForwards(insns, handler);
                analyze(line, insns, nodes, handler, new BitSet());
            }
        } catch (AnalyzerException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Analyzes a basic block starting at insn, recursing for all paths in the CFG, until it finds an exit or
     * throw. records all violations found.
     */
    private void analyze(int line, AbstractInsnNode insns[], List<Node> nodes, int insn, BitSet visited) {
        Node node = nodes.get(insn);
        while (true) {
            if (visited.get(insn)) {
                return;
            }
            visited.set(insn);
            // original exception being added as suppressed exception to another.
            if (isValidSuppression(insns[insn], node)) {
                return;
            }
            // true if its a throw equivalent, and the original is being passed.
            Boolean v = isThrowInsn(insns[insn], node);
            if (v == null) {
                Violation violation = new Violation(Violation.Kind.THROWS_SOMETHING_ELSE_BUT_LOSES_ORIGINAL, line,
                                                    getLineNumberBackwards(insns, insn));
                violations.add(violation);
                return;
            } else if (v) {
                return;
            }
            // end of path, you lose
            if (node.edges.isEmpty()) {
                Violation violation = new Violation(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, line,
                                                    getLineNumberBackwards(insns, insn));
                violations.add(violation);
                return;
            }
            // stay within loop instead of recursing for the rest of this block
            if (node.edges.size() == 1) {
                insn = node.edges.iterator().next();
                node = nodes.get(insn);
            } else {
                break;
            }
        }
        
        // recurse: multiple edges
        for (int edge : node.edges) {
            analyze(line, insns, nodes, edge, visited);
        }
    }
    
    /** true if this is a throw, or an equivalent (e.g. rethrow) */
    private static Boolean isThrowInsn(AbstractInsnNode insn, Node node) {
        if (insn.getOpcode() == Opcodes.ATHROW) {
            if (node.getStack(0) != ThrowableInterpreter.ORIGINAL_THROWABLE) {
                return null;
            }
            return true;
        }
        if (insn instanceof MethodInsnNode) {
            MethodInsnNode method = (MethodInsnNode) insn;
            if (SpecialMethod.isRethrower(method)) {
                for (int i = 0; i < Type.getArgumentTypes(method.desc).length; i++) {
                    if (node.getStack(i) == ThrowableInterpreter.ORIGINAL_THROWABLE) {
                        return true;
                    }
                }
                return null;
            }
        }
        return false;
    }
    
    private boolean isValidSuppression(AbstractInsnNode insn, Node node) {
        if (insn instanceof MethodInsnNode) {
            MethodInsnNode methodNode = (MethodInsnNode)insn;
            if ((methodNode.name.equals("addSuppressed") || methodNode.name.equals("initCause")) &&
                    ThrowableInterpreter.isThrowable(Type.getObjectType(methodNode.owner), loader)) {
                // its a suppressor, check our original is passed
                if (node.getStack(1) == ThrowableInterpreter.ORIGINAL_THROWABLE) {
                    return true;
                }
            }
            // TODO: remove this (private) method from lucene, its really crappy and lenient
            if ((methodNode.name.equals("addSuppressed") && methodNode.owner.equals("org/apache/lucene/util/IOUtils"))) {
                // its a suppressor, check our original is passed
                if (node.getStack(1) == ThrowableInterpreter.ORIGINAL_THROWABLE) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static int getLineNumberForwards(AbstractInsnNode insns[], int insn) {
        // walk forwards in the line number table
        int line = -1;
        for (int i = insn; i < insns.length; i++) {
            AbstractInsnNode next = insns[i];
            if (next instanceof LineNumberNode) {
                line = ((LineNumberNode)next).line;
                break;
            }
        }
        return line;
    }
    
    private static int getLineNumberBackwards(AbstractInsnNode insns[], int insn) {
        // walk backwards in the line number table
        int line = -1;
        for (int i = insn; i >= 0; i--) {
            AbstractInsnNode previous = insns[i];
            if (previous instanceof LineNumberNode) {
                line = ((LineNumberNode)previous).line;
                break;
            }
        }
        return line;
    }
}
