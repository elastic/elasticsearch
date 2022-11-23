/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Simple exception class from a script.
 * <p>
 * Use of this exception should generally be avoided, it doesn't provide
 * much context or structure to users trying to debug scripting when
 * things go wrong.
 * @deprecated Use ScriptException for exceptions from the scripting engine,
 *             otherwise use a more appropriate exception (e.g. if thrown
 *             from various abstractions)
 */
@Deprecated
public class GeneralScriptException extends ElasticsearchException {

    public GeneralScriptException(String msg) {
        super(msg);
    }

    public GeneralScriptException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public GeneralScriptException(StreamInput in) throws IOException {
        super(in);
    }
}
