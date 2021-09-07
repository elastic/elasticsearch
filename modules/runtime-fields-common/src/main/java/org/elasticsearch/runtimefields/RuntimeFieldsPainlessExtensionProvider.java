/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields;

import org.elasticsearch.painless.spi.PainlessExtensionProvider;

/** Runtime fields provider for painless extensions. */
public class RuntimeFieldsPainlessExtensionProvider extends PainlessExtensionProvider {

    public RuntimeFieldsPainlessExtensionProvider() {  // explicit no-args ctr, for ServiceLoader
        super(
            RuntimeFieldsPainlessExtension.class,      // painless extension type
            RuntimeFieldsCommonPlugin.class            // ctr arg type
        );
    }
}
