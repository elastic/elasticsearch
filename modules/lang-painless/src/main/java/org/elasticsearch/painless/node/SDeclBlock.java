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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Represents a series of declarations.
 */
public final class SDeclBlock extends AStatement {

    private final List<SDeclaration> declarations;

    public SDeclBlock(Location location, List<SDeclaration> declarations) {
        super(location);

        this.declarations = Collections.unmodifiableList(declarations);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        for (SDeclaration declaration : declarations) {
            declaration.analyze(scriptRoot, scope);
        }

        statementCount = declarations.size();
    }

    @Override
    DeclarationBlockNode write(ClassNode classNode) {
        DeclarationBlockNode declarationBlockNode = new DeclarationBlockNode();

        for (SDeclaration declaration : declarations) {
            declarationBlockNode.addDeclarationNode(declaration.write(classNode));
        }

        declarationBlockNode.setLocation(location);

        return declarationBlockNode;
    }

    @Override
    public String toString() {
        return multilineToString(emptyList(), declarations);
    }
}
