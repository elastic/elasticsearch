/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DocValuesWhitelistExtension implements PainlessExtension {

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        // TODO: support unsigned_long in scripts
        return Collections.emptyMap();
    }
}
