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

import java.util.function.Consumer;

/**
 * A constant initializer to be added to the class file.
 */
public class Constant {
    public final Location location;
    public final String name;
    public final org.objectweb.asm.Type type;
    public final Consumer<MethodWriter> initializer;
    
    /**
     * Create a new constant.
     *
     * @param location the location in the script that is creating it
     * @param type the type of the constant
     * @param name the name of the constant
     * @param initializer code to initialize the constant. It will be called when generating the clinit method and is expected to leave the
     *        value of the constant on the stack. Generating the load instruction is managed by the caller.
     */
    public Constant(Location location, org.objectweb.asm.Type type, String name, Consumer<MethodWriter> initializer) {
        this.location = location;
        this.name = name;
        this.type = type;
        this.initializer = initializer;
    }
}
