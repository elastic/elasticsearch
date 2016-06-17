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

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.node.SFunction.FunctionReserved;
import org.elasticsearch.painless.Globals;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ELambda extends AExpression implements ILambda {
    final String name;
    final FunctionReserved reserved;
    final List<String> paramTypeStrs;
    final List<String> paramNameStrs;
    final List<AStatement> statements;
    // desugared synthetic method (lambda body)
    SFunction desugared;
    // method ref (impl detail)
    ILambda impl;

    public ELambda(String name, FunctionReserved reserved, 
                   Location location, List<String> paramTypes, List<String> paramNames, 
                   List<AStatement> statements) {
        super(location);
        this.name = name;
        this.reserved = reserved;
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
    }

    @Override
    void analyze(Locals locals) {
        // desugar lambda body into a synthetic method
        desugared = new SFunction(reserved, location, "def", name, 
                                            paramTypeStrs, paramNameStrs, statements, true);
        desugared.generate();
        List<Variable> captures = new ArrayList<>();
        desugared.analyze(Locals.newLambdaScope(locals.getProgramScope(), desugared.parameters, captures));
        
        // setup reference
        EFunctionRef ref = new EFunctionRef(location, "this", name);
        ref.expected = expected;
        // hack, create a new scope, with our method, so the ref can see it (impl detail)
        locals = Locals.newLocalScope(locals);
        locals.addMethod(desugared.method);
        ref.analyze(locals);
        actual = ref.actual;
        impl = ref;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        AExpression expr = (AExpression) impl;
        expr.write(writer, globals);
        // add synthetic method to the queue to be written
        globals.addSyntheticMethod(desugared);
    }

    @Override
    public String getPointer() {
        return impl.getPointer();
    }

    @Override
    public Type[] getCaptures() {
        return impl.getCaptures();
    }
    
}
