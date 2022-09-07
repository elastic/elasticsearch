/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.io.IOException;

/**
 * A string template rendered as a script.
 */
public abstract class BinaryScript<T> {

    public BinaryScript() {}

    /** Compile a script and return the resulting compiled representation of the script. */
    public abstract T execute();

    /** A factory to construct {@link BinaryScript} instances. */
    public interface LeafFactory<T> {
        BinaryScript<T> newInstance() throws IOException;
    }

    public interface Factory<T> extends ScriptFactory {
        BinaryScript.LeafFactory<T> newFactory();
    }

    @SuppressWarnings("rawtypes")
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("binary", Factory.class);
}
