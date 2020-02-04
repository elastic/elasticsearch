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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.objectweb.asm.util.Printer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public final class SClass extends ANode {

    private final ScriptClassInfo scriptClassInfo;
    private final String name;
    private final Printer debugStream;
    private final List<SFunction> functions = new ArrayList<>();
    private final List<SField> fields = new ArrayList<>();

    private ScriptRoot scriptRoot;
    private final String sourceText;

    public SClass(ScriptClassInfo scriptClassInfo, String name, String sourceText, Printer debugStream,
            Location location, List<SFunction> functions) {
        super(location);
        this.scriptClassInfo = Objects.requireNonNull(scriptClassInfo);
        this.name = Objects.requireNonNull(name);
        this.debugStream = debugStream;
        this.functions.addAll(Objects.requireNonNull(functions));
        this.sourceText = Objects.requireNonNull(sourceText);
    }

    void addFunction(SFunction function) {
        functions.add(function);
    }

    void addField(SField field) {
        fields.add(field);
    }

    public ScriptRoot analyze(PainlessLookup painlessLookup, CompilerSettings settings) {
        scriptRoot = new ScriptRoot(painlessLookup, settings, scriptClassInfo, this);

        for (SFunction function : functions) {
            function.generateSignature(painlessLookup);

            String key = FunctionTable.buildLocalFunctionKey(function.name, function.typeParameters.size());

            if (scriptRoot.getFunctionTable().getFunction(key) != null) {
                throw createError(new IllegalArgumentException("Illegal duplicate functions [" + key + "]."));
            }

            scriptRoot.getFunctionTable().addFunction(
                    function.name, function.returnType, function.typeParameters, function.isInternal, function.isStatic);
        }

        // copy protection is required because synthetic functions are
        // added for lambdas/method references and analysis here is
        // only for user-defined functions
        List<SFunction> functions = new ArrayList<>(this.functions);
        for (SFunction function : functions) {
            function.analyze(scriptRoot);
        }

        return scriptRoot;
    }

    @Override
    public StatementNode write(ClassNode classNode) {
        throw new UnsupportedOperationException();
    }

    public ClassNode writeClass() {
        ClassNode classNode = new ClassNode();

        for (SField field : fields) {
            classNode.addFieldNode(field.write(classNode));
        }

        for (SFunction function : functions) {
            classNode.addFunctionNode(function.write(classNode));
        }

        classNode.setLocation(location);
        classNode.setScriptClassInfo(scriptClassInfo);
        classNode.setScriptRoot(scriptRoot);
        classNode.setDebugStream(debugStream);
        classNode.setName(name);
        classNode.setSourceText(sourceText);

        return classNode;
    }

    @Override
    public String toString() {
        List<Object> subs = new ArrayList<>(functions.size());
        subs.addAll(functions);
        return multilineToString(emptyList(), subs);
    }
}
