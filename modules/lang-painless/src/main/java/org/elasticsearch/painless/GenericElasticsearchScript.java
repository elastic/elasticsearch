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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.MainMethod.DerivedArgument;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.objectweb.asm.Opcodes;

import java.util.Map;

import static org.elasticsearch.painless.WriterConstants.MAP_GET;
import static org.elasticsearch.painless.WriterConstants.MAP_TYPE;

/**
 * Generic script interface that all scripts that elasticsearch uses implement.
 */
@FunctionalInterface
public interface GenericElasticsearchScript {
    Object execute(
            @Arg(            name="params")  Map<String, Object> params,
            @Arg(type="def", name="#scorer") Scorer scorer,
            @Arg(type="Map", name="doc")     LeafDocLookup doc,
            @Arg(type="def", name="_value")  Object value);

    DerivedArgument[] DERIVED_ARGUMENTS = new DerivedArgument[] {
        new DerivedArgument(Definition.DOUBLE_TYPE, "_score", (writer, locals) -> {
            // If _score is used in the script then run this before any user code:
            // final double _score = scorer.score();
            Variable scorer = locals.getVariable(null, "#scorer");
            Variable score = locals.getVariable(null, "_score");

            writer.visitVarInsn(Opcodes.ALOAD, scorer.getSlot());
            writer.invokeVirtual(WriterConstants.SCORER_TYPE, WriterConstants.SCORER_SCORE);
            writer.visitInsn(Opcodes.F2D);
            writer.visitVarInsn(Opcodes.DSTORE, score.getSlot());
        }),
        new DerivedArgument(Definition.getType("Map"), "ctx", (writer, locals) -> {
            // If ctx is used in the script then run this before any user code:
            // final Map<String,Object> ctx = input.get("ctx");
            Variable params = locals.getVariable(null, "params");
            Variable ctx = locals.getVariable(null, "ctx");

            writer.visitVarInsn(Opcodes.ALOAD, params.getSlot());
            writer.push("ctx");
            writer.invokeInterface(MAP_TYPE, MAP_GET);
            writer.visitVarInsn(Opcodes.ASTORE, ctx.getSlot());
        })
    };
}
