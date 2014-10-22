/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import java.nio.charset.Charset;

public class LicensesCharset {

    /**
     * All operations in should use the universal UTF-8 character set.
     */
    public static final Charset UTF_8 = Charset.forName("UTF-8");

}
