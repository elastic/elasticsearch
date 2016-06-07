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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Variables;

import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;

import java.lang.invoke.LambdaMetafactory;

/**
 * Represents a function reference.
 */
public class EFunctionRef extends AExpression {
    public final String type;
    public final String call;
    
    private FunctionRef ref;

    public EFunctionRef(Location location, String type, String call) {
        super(location);

        this.type = type;
        this.call = call;
    }

    @Override
    void analyze(Variables variables) {
        if (expected == null) {
            ref = null;
            actual = Definition.getType("String");
        } else {
            try {
                ref = new FunctionRef(expected, type, call);
            } catch (IllegalArgumentException e) {
                throw createError(e);
            }
            actual = expected;
        }
    }

    @Override
    void write(MethodWriter writer) {
        if (ref == null) {
            writer.push(type + "." + call);
        } else {
            writer.writeDebugInfo(location);
            // currently if the interface differs, we ask for a bridge, but maybe we should do smarter checking?
            // either way, stuff will fail if its wrong :)
            if (ref.interfaceType.equals(ref.samType)) {
                writer.invokeDynamic(ref.invokedName, ref.invokedType.getDescriptor(), LAMBDA_BOOTSTRAP_HANDLE, 
                                     ref.samType, ref.implMethod, ref.samType, 0);
            } else {
                writer.invokeDynamic(ref.invokedName, ref.invokedType.getDescriptor(), LAMBDA_BOOTSTRAP_HANDLE, 
                                     ref.samType, ref.implMethod, ref.samType, 
                                     LambdaMetafactory.FLAG_BRIDGES, 1, ref.interfaceType);
            }
        }
    }
}
