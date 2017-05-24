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

package org.elasticsearch.script;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The information necessary to compile and run a script.
 *
 * A {@link ScriptContext} contains the information related to a single use case and the interfaces
 * and methods necessary for a {@link ScriptEngine} to implement.
 * <p>
 * There are two related classes which must be supplied to construct a {@link ScriptContext}.
 * <p>
 * The <i>InstanceType</i> is a class, possibly stateful, that is an instance of a script. Instances of
 * the <i>InstanceType</i> may be executed multiple times by a caller with different arguments. This
 * class must have an abstract method named <i>execute</i> which {@link ScriptEngine} implementations
 * will define.
 * <p>
 * The <i>CompiledType</i> is a factory class for constructing instances of a script. The
 * {@link ScriptService} returns an instance of <i>CompiledType</i> when compiling a script. This class
 * must be stateless so it is cacheable by the {@link ScriptService}. It must have an abstract method
 * named <i>newInstance</i> which returns <i>InstanceType</i> which {@link ScriptEngine} implementations
 * will define.
 */
public final class ScriptContext<InstanceType, CompiledType> {

    public static final ScriptContext<SearchScript, SearchScript.Compiled> AGGS =
        new ScriptContext<>("aggs", SearchScript.class, SearchScript.Compiled.class);
    public static final ScriptContext<SearchScript, SearchScript.Compiled> SEARCH =
        new ScriptContext<>("search", SearchScript.class, SearchScript.Compiled.class);
    // TODO: remove this once each agg calling scripts has its own context
    public static final ScriptContext<ExecutableScript, ExecutableScript.Compiled> AGGS_EXECUTABLE =
        new ScriptContext<>("aggs_executable", ExecutableScript.class, ExecutableScript.Compiled.class);
    public static final ScriptContext<ExecutableScript, ExecutableScript.Compiled> UPDATE =
        new ScriptContext<>("update", ExecutableScript.class, ExecutableScript.Compiled.class);
    public static final ScriptContext<ExecutableScript, ExecutableScript.Compiled> INGEST =
        new ScriptContext<>("ingest", ExecutableScript.class, ExecutableScript.Compiled.class);
    public static final ScriptContext<ExecutableScript, ExecutableScript.Compiled> EXECUTABLE =
        new ScriptContext<>("executable", ExecutableScript.class, ExecutableScript.Compiled.class);

    public static final Map<String, ScriptContext<?, ?>> BUILTINS;
    static {
        Map<String, ScriptContext<?, ?>> builtins = new HashMap<>();
        builtins.put(AGGS.name, AGGS);
        builtins.put(SEARCH.name, SEARCH);
        builtins.put(AGGS_EXECUTABLE.name, AGGS_EXECUTABLE);
        builtins.put(UPDATE.name, UPDATE);
        builtins.put(INGEST.name, INGEST);
        builtins.put(EXECUTABLE.name, EXECUTABLE);
        BUILTINS = Collections.unmodifiableMap(builtins);
    }

    /** A unique identifier for this context. */
    public final String name;

    /** A class that is an instance of a script. */
    public final Class<InstanceType> instanceClazz;

    /** A factory class for constructing instances of a script. */
    public final Class<CompiledType> compiledClazz;

    /** Construct a context with the related instance and compiled classes. */
    public ScriptContext(String name, Class<InstanceType> instanceClazz, Class<CompiledType> compiledClazz) {
        this.name = name;
        this.instanceClazz = instanceClazz;
        this.compiledClazz = compiledClazz;
    }
}
