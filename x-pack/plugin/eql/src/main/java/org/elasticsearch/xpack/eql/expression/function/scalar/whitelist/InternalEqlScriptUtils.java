/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.whitelist;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.SubstringFunctionProcessor;
import org.elasticsearch.xpack.ql.expression.function.scalar.whitelist.InternalQlScriptUtils;

/*
 * Whitelisted class for EQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
public class InternalEqlScriptUtils extends InternalQlScriptUtils {

    InternalEqlScriptUtils() {}

    public static String substring(String s, Number start, Number end) {
        return (String) SubstringFunctionProcessor.doProcess(s, start, end);
    }
}
