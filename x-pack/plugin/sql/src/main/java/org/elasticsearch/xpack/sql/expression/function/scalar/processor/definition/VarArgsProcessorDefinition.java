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
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.BinaryArithmeticProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class VarArgsProcessorDefinition extends ProcessorDefinition {

    private final List<ProcessorDefinition> definitions;

    protected VarArgsProcessorDefinition(Location location, Expression expression, List<ProcessorDefinition> definitions) {
        super(location, expression, definitions);
        this.definitions = definitions;
    }

    @Override
    public final boolean resolved() {
        return this.definitions.stream()
            .allMatch(d -> d.resolved());
    }

    @Override
    public final Processor asProcessor() {
        return this.asProcessor(this.definitions.stream()
            .map(d -> d.asProcessor())
            .collect(Collectors.toList()));
    }

    protected abstract Processor asProcessor(List<Processor> processors);

    @Override
    public final ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        List<ProcessorDefinition> resolved = this.definitions.stream()
            .map(d -> d.resolveAttributes(resolver))
            .collect(Collectors.toList());
        return this.definitions.equals(resolved) ? this : replaceChildren(resolved);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        this.definitions.stream()
            .forEach(d -> d.collectFields(sourceBuilder));
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return this.definitions.stream()
            .allMatch(d -> d.supportedByAggsOnlyQuery());
    }
}
