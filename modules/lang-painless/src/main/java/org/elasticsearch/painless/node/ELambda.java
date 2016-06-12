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
import org.elasticsearch.painless.Locals.FunctionReserved;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Collections;
import java.util.List;

public class ELambda extends AExpression {
    final FunctionReserved reserved;
    final List<String> paramTypeStrs;
    final List<String> paramNameStrs;
    final List<AStatement> statements;

    public ELambda(FunctionReserved reserved, Location location,
                   List<String> paramTypes, List<String> paramNames, List<AStatement> statements) {
        super(location);

        this.reserved = reserved;
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
    }

    @Override
    void analyze(Locals locals) {
        throw createError(new UnsupportedOperationException("Lambda functions are not supported."));
    }

    @Override
    void write(MethodWriter writer) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
