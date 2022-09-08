/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * A script compiled into it's raw executable form
 * <p>
 * The raw executable script can be used by other scripting engines as a
 * base for their internal execution. This class provides the means for building
 * scripting engines on top of other scripting engines.
 */
public abstract class RawScript<T> {

    public RawScript() {}

    /** Compile a script and return the resulting compiled representation of the script. */
    public abstract T execute();

    /** A factory to construct {@link RawScript} instances. */
    public interface Factory<T> extends ScriptFactory {
        RawScript<T> newInstance();
    }

    @SuppressWarnings("rawtypes")
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("raw", Factory.class);
}
