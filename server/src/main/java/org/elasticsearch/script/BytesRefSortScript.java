/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import java.io.IOException;
import java.util.Map;

public abstract class BytesRefSortScript extends AbstractSortScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("bytesref_sort", Factory.class);

    public BytesRefSortScript(Map<String, Object> params, DocReader docReader) {
        super(params, docReader);
    }

    public abstract Object execute();

    /**
     * A factory to construct {@link BytesRefSortScript} instances.
     */
    public interface LeafFactory {
        BytesRefSortScript newInstance(DocReader reader) throws IOException;
    }

    /**
     * A factory to construct stateful {@link BytesRefSortScript} factories for a specific index.
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params);
    }
}
