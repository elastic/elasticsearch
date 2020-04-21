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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public class SClass extends ANode {

    protected final List<SFunction> functions = new ArrayList<>();

    public SClass(Location location, List<SFunction> functions) {
        super(location);
        this.functions.addAll(Objects.requireNonNull(functions));
    }

    public void buildClassScope(ScriptRoot scriptRoot) {
        for (SFunction function : functions) {
            function.buildClassScope(scriptRoot);
        }
    }

    public ClassNode writeClass(ScriptRoot scriptRoot) {
        buildClassScope(scriptRoot);

        ClassNode classNode = new ClassNode();

        for (SFunction function : functions) {
            classNode.addFunctionNode(function.writeFunction(classNode, scriptRoot));
        }

        classNode.setLocation(location);
        classNode.setScriptRoot(scriptRoot);

        return classNode;
    }
}
