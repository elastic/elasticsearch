/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index;

import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.shield.authz.accesscontrol.ShieldIndexSearcherWrapper;

/**
 * This class installs package protected extension points on the {@link IndexModule}
 */
public class ProtectedServiceInstaller {

    public static void install(IndexModule module, boolean clientMode) {
        module.indexSearcherWrapper = ShieldIndexSearcherWrapper.class;
        if (clientMode == false) {
            module.registerQueryCache(ShieldPlugin.OPT_OUT_QUERY_CACHE, OptOutQueryCache::new);
        }
    }

}
