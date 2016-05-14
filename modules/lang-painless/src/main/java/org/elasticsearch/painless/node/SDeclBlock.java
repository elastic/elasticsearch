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
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

/**
 * Represents a series of declarations.
 */
public final class SDeclBlock extends AStatement {

    final List<SDeclaration> declarations;

    public SDeclBlock(final int line, final String location, final List<SDeclaration> declarations) {
        super(line, location);

        this.declarations = Collections.unmodifiableList(declarations);
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        for (final SDeclaration declaration : declarations) {
            declaration.analyze(settings, definition, variables);
        }

        statementCount = declarations.size();
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        for (final SDeclaration declaration : declarations) {
            declaration.write(settings, definition, adapter);
        }
    }
}
