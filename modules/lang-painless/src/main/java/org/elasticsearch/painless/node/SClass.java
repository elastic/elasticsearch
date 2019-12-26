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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope.FunctionScope;
import org.elasticsearch.painless.Scope.LocalScope;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.ScriptClassInfo.MethodArgument;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.objectweb.asm.util.Printer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
    private final List<AStatement> statements;

    private CompilerSettings settings;

    private ScriptRoot scriptRoot;
    private final Set<String> extractedVariables;
    private final String sourceText;

    private boolean methodEscape;
    private boolean allEscape;

    public SClass(ScriptClassInfo scriptClassInfo, String name, String sourceText, Printer debugStream,
            Location location, List<SFunction> functions, List<AStatement> statements) {
        super(location);
        this.scriptClassInfo = Objects.requireNonNull(scriptClassInfo);
        this.name = Objects.requireNonNull(name);
        this.debugStream = debugStream;
        this.functions.addAll(Objects.requireNonNull(functions));
        this.statements = Collections.unmodifiableList(statements);
        this.sourceText = Objects.requireNonNull(sourceText);

        this.extractedVariables = new HashSet<>();
    }

    void addFunction(SFunction function) {
        functions.add(function);
    }

    void addField(SField field) {
        fields.add(field);
    }

    @Override
    public void extractVariables(Set<String> variables) {
        for (SFunction function : functions) {
            function.extractVariables(null);
        }

        for (AStatement statement : statements) {
            statement.extractVariables(variables);
        }

        extractedVariables.addAll(variables);
    }

    public ScriptRoot analyze(PainlessLookup painlessLookup, CompilerSettings settings) {
        this.settings = settings;
        scriptRoot = new ScriptRoot(painlessLookup, settings, scriptClassInfo, this);

        for (SFunction function : functions) {
            function.generateSignature(painlessLookup);

            String key = FunctionTable.buildLocalFunctionKey(function.name, function.typeParameters.size());

            if (scriptRoot.getFunctionTable().getFunction(key) != null) {
                throw createError(new IllegalArgumentException("Illegal duplicate functions [" + key + "]."));
            }

            scriptRoot.getFunctionTable().addFunction(function.name, function.returnType, function.typeParameters, false);
        }

        // copy protection is required because synthetic functions are
        // added for lambdas/method references and analysis here is
        // only for user-defined functions
        List<SFunction> functions = new ArrayList<>(this.functions);
        for (SFunction function : functions) {
            function.analyze(scriptRoot);
        }

        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty script."));
        }

        FunctionScope functionScope = Scope.newFunctionScope(scriptClassInfo.getExecuteMethodReturnType());

        for (MethodArgument argument : scriptClassInfo.getExecuteArguments()) {
            functionScope.defineVariable(
                    new Location("execute [" + argument.getName() + "]", 0), argument.getClazz(), argument.getName(), true);
        }

        for (int get = 0; get < scriptClassInfo.getGetMethods().size(); ++get) {
            org.objectweb.asm.commons.Method method = scriptClassInfo.getGetMethods().get(get);
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            Class<?> rtn = scriptClassInfo.getGetReturns().get(get);
            functionScope.defineVariable(new Location("getter [" + name + "]", 0), rtn, name, true);
        }

        LocalScope localScope = functionScope.newLocalScope();

        AStatement last = statements.get(statements.size() - 1);

        for (AStatement statement : statements) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            statement.lastSource = statement == last;
            statement.analyze(scriptRoot, localScope);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
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

        for (AStatement statement : statements) {
            classNode.addStatementNode(statement.write(classNode));
        }

        classNode.setLocation(location);
        classNode.setScriptClassInfo(scriptClassInfo);
        classNode.setScriptRoot(scriptRoot);
        classNode.setDebugStream(debugStream);
        classNode.setName(name);
        classNode.setSourceText(sourceText);
        classNode.setMethodEscape(methodEscape);
        classNode.getExtractedVariables().addAll(extractedVariables);

        return classNode;
    }

    @Override
    public String toString() {
        List<Object> subs = new ArrayList<>(functions.size() + statements.size());
        subs.addAll(functions);
        subs.addAll(statements);
        return multilineToString(emptyList(), subs);
    }
}
