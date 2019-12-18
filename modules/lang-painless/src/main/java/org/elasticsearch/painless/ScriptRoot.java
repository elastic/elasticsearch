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

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.symbol.FunctionTable;

import java.util.Objects;

/**
 * Stores information for use across the entirety of compilation.
 */
public class ScriptRoot {

    protected final PainlessLookup painlessLookup;
    protected final CompilerSettings compilerSettings;
    protected final ScriptClassInfo scriptClassInfo;

    protected final SClass classNode;

    protected final FunctionTable functionTable = new FunctionTable();
    protected int syntheticCounter = 0;
    protected boolean deterministic = true;

    public ScriptRoot(PainlessLookup painlessLookup, CompilerSettings compilerSettings, ScriptClassInfo scriptClassInfo, SClass classRoot) {
        this.painlessLookup = Objects.requireNonNull(painlessLookup);
        this.compilerSettings = Objects.requireNonNull(compilerSettings);
        this.scriptClassInfo = Objects.requireNonNull(scriptClassInfo);
        this.classNode = Objects.requireNonNull(classRoot);
    }

    public PainlessLookup getPainlessLookup() {
        return painlessLookup;
    }

    public CompilerSettings getCompilerSettings() {
        return compilerSettings;
    }

    public ScriptClassInfo getScriptClassInfo() {
        return scriptClassInfo;
    }

    public SClass getClassNode() {
        return classNode;
    }

    public FunctionTable getFunctionTable() {
        return functionTable;
    }

    /**
     * Returns a unique identifier for generating the name of a synthetic value.
     */
    public String getNextSyntheticName(String prefix) {
        return prefix + "$synthetic$" + syntheticCounter++;
    }

    public void markNonDeterministic(boolean nondeterministic) { this.deterministic &= !nondeterministic; }
}
