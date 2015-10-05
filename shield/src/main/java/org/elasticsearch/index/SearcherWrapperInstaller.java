/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index;

import org.elasticsearch.shield.authz.accesscontrol.ShieldIndexSearcherWrapper;

public class SearcherWrapperInstaller {

    public static void install(IndexModule module) {
        module.indexSearcherWrapper = ShieldIndexSearcherWrapper.class;
    }

}
