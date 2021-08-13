/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
