/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

public class StandardLevels {

    public static final int OFF = 0;

    public static final int FATAL = 100;

    public static final int ERROR = 200;

    public static final int WARN = 300;

    public static final int INFO = 400;

    public static final int DEBUG = 500;

    public static final int TRACE = 600;

    public static final int ALL = Integer.MAX_VALUE;

    private StandardLevels() {}

}
